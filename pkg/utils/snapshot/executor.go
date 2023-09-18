/*
Copyright The CloudNativePG Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	storagesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/log"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/resources"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
)

var snapshotBackoff = wait.Backoff{
	Steps:    200,
	Duration: 5 * time.Second,
	Factor:   1.0,
	Jitter:   0.1,
}

// Executor is an object capable of executing a volume snapshot on a running cluster
type Executor struct {
	cli                client.Client
	shouldFence        bool
	snapshotSuffix     string
	snapshotEnrichFunc func(vs *storagesnapshotv1.VolumeSnapshot)
	snapshotConfig     apiv1.VolumeSnapshotConfiguration
}

// ExecutorBuilder is a struct capable of creating an Executor
type ExecutorBuilder struct {
	executor Executor
}

// NewExecutorBuilder instantiates a new ExecutorBuilder with the minimum required data
func NewExecutorBuilder(
	cli client.Client,
	config apiv1.VolumeSnapshotConfiguration,
) *ExecutorBuilder {
	return &ExecutorBuilder{
		executor: Executor{
			cli:                cli,
			snapshotEnrichFunc: func(vs *storagesnapshotv1.VolumeSnapshot) {},
			snapshotConfig:     config,
		},
	}
}

// FenceInstance instructs if the Executor should fence or not the instance while taking the snapshot
func (e *ExecutorBuilder) FenceInstance(fence bool) *ExecutorBuilder {
	e.executor.shouldFence = fence
	return e
}

// WithSnapshotEnrich accepts a function capable of adding new data to the storagesnapshotv1.VolumeSnapshot resource
func (e *ExecutorBuilder) WithSnapshotEnrich(enrich func(vs *storagesnapshotv1.VolumeSnapshot)) *ExecutorBuilder {
	e.executor.snapshotEnrichFunc = enrich
	return e
}

// WithSnapshotSuffix the suffix that should be added to the snapshots. Defaults to unix timestamp.
func (e *ExecutorBuilder) WithSnapshotSuffix(suffix string) *ExecutorBuilder {
	e.executor.snapshotSuffix = suffix
	return e
}

// Build returns the Executor instance
func (e *ExecutorBuilder) Build() *Executor {
	return &e.executor
}

// Execute the volume snapshot of the given cluster instance
func (se *Executor) Execute(
	ctx context.Context,
	cluster *apiv1.Cluster,
	targetPod *corev1.Pod,
	pvcs []corev1.PersistentVolumeClaim,
	backupIdentifier string,
) (*ctrl.Result, error) {
	contextLogger := log.FromContext(ctx).WithValues("podName", targetPod.Name)

	// If there's another snapshot running, but it's not the backup we're reconciling,
	// we need to wait for it to finish
	if validateNoOtherOngoingBackup(backupIdentifier)(cluster) != nil {
		contextLogger.Info(
			"A backup is already in progress, retrying",
			"runningSnapshots", cluster.Status.RunningBackups.Snapshots)
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	created, err := se.ensureOngoingBackupIsRegistered(ctx, cluster, backupIdentifier)
	if err != nil {
		return nil, err
	}

	if se.shouldFence && created {
		contextLogger.Info("LEO3", "created", created, "err", err, "fence", se.shouldFence)
		contextLogger.Debug("Checking pre-requisites")
		if err := se.checkPreconditionsStep(ctx, cluster); err != nil {
			return nil, err
		}

		contextLogger.Info("LEO4", "created", created, "err", err, "fence", se.shouldFence, "targetPod", targetPod)
		if err := se.fencePodStep(ctx, cluster, targetPod); err != nil {
			return nil, err
		}

		contextLogger.Info("LEO5", "created", created, "err", err, "fence", se.shouldFence, "targetPod", targetPod)
	}

	contextLogger.Info("LEO6", "created", created, "err", err, "fence", se.shouldFence, "targetPod", targetPod)
	if se.shouldFence {
		if res, err := se.ensurePodToBeFencedStep(ctx, targetPod); res != nil || err != nil {
			contextLogger.Info("LEO6.2", "res", res, "err", err)
			return res, err
		}
	}

	// if we have no suffix specified from the user we use unix timestamp
	if se.snapshotSuffix == "" {
		se.snapshotSuffix = fmt.Sprintf("%d", time.Now().Unix())
	}

	volumeSnapshots, err := GetBackupVolumeSnapshots(ctx, se.cli, cluster.Namespace, backupIdentifier)
	if err != nil {
		return nil, err
	}
	// we execute the snapshots only if we don't find any
	if len(volumeSnapshots) == 0 {
		contextLogger.Debug("Creating VolumeSnapshots")
		// TODO: handle the case that the pvc were already made by another process
		if err := se.snapshotPVCGroupStep(ctx, pvcs); err != nil {
			return nil, err
		}
	}

	contextLogger.Debug("Waiting to snapshots to be ready")
	if err := se.waitSnapshotToBeReadyStep(ctx, pvcs); err != nil {
		return nil, err
	}

	se.EnsurePodIsUnfenced(ctx, cluster, targetPod)

	return nil, se.tryUpdateClusterStatus(ctx,
		cluster.Name,
		cluster.Namespace,
		validateNoOtherOngoingBackup(backupIdentifier),
		func(latestCluster *apiv1.Cluster) {
			delete(latestCluster.Status.RunningBackups.Snapshots, backupIdentifier)
		})
}

func (se *Executor) ensureOngoingBackupIsRegistered(
	ctx context.Context,
	cluster *apiv1.Cluster,
	name string,
) (created bool, err error) {
	if _, ok := cluster.Status.RunningBackups.Snapshots[name]; ok {
		return false, nil
	}

	return true, se.tryUpdateClusterStatus(
		ctx,
		cluster.Name,
		cluster.Namespace,
		validateOngoingBackupNotPresent(name),
		func(cluster *apiv1.Cluster) {
			if cluster.Status.RunningBackups.Snapshots == nil {
				cluster.Status.RunningBackups.Snapshots = make(map[string]apiv1.RunningSnapshotBackup)
			}
			cluster.Status.RunningBackups.Snapshots[name] = apiv1.RunningSnapshotBackup{
				Online: false,
				Status: apiv1.BackupPhaseRunning,
			}
		},
	)
}

// checkPreconditionsStep checks if the preconditions for the execution of this step are
// met or not. If they are not met, it will return an error
func (se *Executor) checkPreconditionsStep(
	ctx context.Context,
	cluster *apiv1.Cluster,
) error {
	contextLogger := log.FromContext(ctx)
	contextLogger.Debug("Ensuring that no pod is fenced before starting")

	fencedInstances, err := utils.GetFencedInstances(cluster.Annotations)
	if err != nil {
		return fmt.Errorf("could not check if cluster is fenced: %v", err)
	}

	if fencedInstances.Len() > 0 {
		return errors.New("cannot execute volume snapshot on a cluster that has fenced instances")
	}

	return nil
}

// fencePodStep fence the target Pod
func (se *Executor) fencePodStep(
	ctx context.Context,
	cluster *apiv1.Cluster,
	targetPod *corev1.Pod,
) error {
	contextLogger := log.FromContext(ctx)
	contextLogger.Info("Fencing Pod")

	return resources.ApplyFenceFunc(
		ctx,
		se.cli,
		cluster.Name,
		cluster.Namespace,
		targetPod.Name,
		utils.AddFencedInstance,
	)
}

// EnsurePodIsUnfenced removes the fencing status from the cluster
func (se *Executor) EnsurePodIsUnfenced(
	ctx context.Context,
	cluster *apiv1.Cluster,
	targetPod *corev1.Pod,
) {
	contextLogger := log.FromContext(ctx)
	contextLogger.Info("Unfencing Pod")

	if err := resources.ApplyFenceFunc(
		ctx,
		se.cli,
		cluster.Name,
		cluster.Namespace,
		targetPod.Name,
		utils.RemoveFencedInstance,
	); err != nil {
		contextLogger.Error(err, "while rolling back the pod from the fencing state")
	}
}

// ensurePodToBeFencedStep waits for the target Pod to be shut down
func (se *Executor) ensurePodToBeFencedStep(
	ctx context.Context,
	targetPod *corev1.Pod,
) (*ctrl.Result, error) {
	contextLogger := log.FromContext(ctx)
	contextLogger.Info("Waiting for Pod to be fenced")

	var pod corev1.Pod
	err := se.cli.Get(ctx, types.NamespacedName{Name: targetPod.Name, Namespace: targetPod.Namespace}, &pod)
	if err != nil {
		return nil, err
	}
	ready := utils.IsPodReady(pod)
	if ready {
		contextLogger.Info("Instance is still running, retrying")
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return nil, nil
}

// snapshotPVCGroup creates a volumeSnapshot resource for every PVC
// used by the Pod
func (se *Executor) snapshotPVCGroupStep(
	ctx context.Context,
	pvcs []corev1.PersistentVolumeClaim,
) error {
	for i := range pvcs {
		err := se.createSnapshot(ctx, &pvcs[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// waitSnapshotToBeReadyStep waits for every PVC snapshot to be ready to use
func (se *Executor) waitSnapshotToBeReadyStep(
	ctx context.Context,
	pvcs []corev1.PersistentVolumeClaim,
) error {
	for i := range pvcs {
		name := se.getSnapshotName(pvcs[i].Name)
		if err := se.waitSnapshot(ctx, name, pvcs[i].Namespace); err != nil {
			return err
		}
	}

	return nil
}

// createSnapshot creates a VolumeSnapshot resource for the given PVC and
// add it to the command status
func (se *Executor) createSnapshot(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) error {
	name := se.getSnapshotName(pvc.Name)
	var snapshotClassName *string
	role := utils.PVCRole(pvc.Labels[utils.PvcRoleLabelName])
	if role == utils.PVCRolePgWal && se.snapshotConfig.WalClassName != "" {
		snapshotClassName = &se.snapshotConfig.WalClassName
	}

	// this is the default value if nothing else was assigned
	if snapshotClassName == nil && se.snapshotConfig.ClassName != "" {
		snapshotClassName = &se.snapshotConfig.ClassName
	}

	labels := pvc.Labels
	utils.MergeMap(labels, se.snapshotConfig.Labels)
	annotations := pvc.Annotations
	utils.MergeMap(annotations, se.snapshotConfig.Annotations)

	snapshot := storagesnapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   pvc.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: storagesnapshotv1.VolumeSnapshotSpec{
			Source: storagesnapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvc.Name,
			},
			VolumeSnapshotClassName: snapshotClassName,
		},
	}
	if snapshot.Labels == nil {
		snapshot.Labels = map[string]string{}
	}
	if snapshot.Annotations == nil {
		snapshot.Annotations = map[string]string{}
	}

	se.snapshotEnrichFunc(&snapshot)

	err := se.cli.Create(ctx, &snapshot)
	if err != nil {
		return fmt.Errorf("while creating VolumeSnapshot %s: %w", snapshot.Name, err)
	}

	return nil
}

// waitSnapshot waits for a certain snapshot to be ready to use
func (se *Executor) waitSnapshot(ctx context.Context, name, namespace string) error {
	contextLogger := log.FromContext(ctx)
	contextLogger.Info("Waiting for VolumeSnapshot to be ready to use", "volumeSnapshotName", name)

	return retry.OnError(snapshotBackoff, resources.RetryAlways, func() error {
		var snapshot storagesnapshotv1.VolumeSnapshot

		err := se.cli.Get(
			ctx,
			client.ObjectKey{
				Namespace: namespace,
				Name:      name,
			},
			&snapshot,
		)
		if err != nil {
			return fmt.Errorf("snapshot %s is not available: %w", name, err)
		}

		if snapshot.Status != nil && snapshot.Status.Error != nil {
			return fmt.Errorf("snapshot %s is not ready to use.\nError: %v", name, snapshot.Status.Error.Message)
		}

		if snapshot.Status == nil || snapshot.Status.ReadyToUse == nil || !*snapshot.Status.ReadyToUse {
			return fmt.Errorf("snapshot %s is not ready to use", name)
		}

		return nil
	})
}

// getSnapshotName gets the snapshot name for a certain PVC
func (se *Executor) getSnapshotName(pvcName string) string {
	return fmt.Sprintf("%s-%s", pvcName, se.snapshotSuffix)
}

func (se *Executor) tryUpdateClusterStatus(
	ctx context.Context,
	clusterName string,
	namespace string,
	validator func(*apiv1.Cluster) error,
	mutator func(*apiv1.Cluster),
) error {
	backoff := wait.Backoff{
		Duration: 2,
		Factor:   1,
		Jitter:   1,
		Steps:    5,
	}

	return retry.RetryOnConflict(backoff, func() error {
		var latestCluster apiv1.Cluster
		getErr := se.cli.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      clusterName,
		}, &latestCluster)
		if getErr != nil {
			return getErr
		}

		if err := validator(&latestCluster); err != nil {
			return err
		}

		mutator(&latestCluster)

		return se.cli.Status().Update(ctx, &latestCluster)
	})
}

func validateNoOtherOngoingBackup(ongoingName string) func(cluster *apiv1.Cluster) error {
	return func(cluster *apiv1.Cluster) error {
		_, backupInProgress := cluster.Status.RunningBackups.Snapshots[ongoingName]

		switch {
		case backupInProgress && len(cluster.Status.RunningBackups.Snapshots) > 1:
			return fmt.Errorf("another backup is being ran")
		case !backupInProgress && len(cluster.Status.RunningBackups.Snapshots) > 0:
			return fmt.Errorf("another backup is being ran")
		}
		return nil
	}
}

func validateOngoingBackupNotPresent(ongoingName string) func(cluster *apiv1.Cluster) error {
	return func(cluster *apiv1.Cluster) error {
		if _, ok := cluster.Status.RunningBackups.Snapshots[ongoingName]; ok {
			return fmt.Errorf("the backup is already present")
		}
		return nil
	}
}
