package snapshot

import (
	"context"
	"errors"
	"fmt"

	storagesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/log"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
)

// ErrUnexpectedFencedInstances is raised when there are more than one Pod fenced or when
// there are no Pod fenced at all.
var ErrUnexpectedFencedInstances = errors.New("expected one and only one instance to be fenced")

// VolumeSnapshotInfo host information about a volume snapshot
type VolumeSnapshotInfo struct {
	// Error contains the raised error when the volume snapshot terminated
	// with a failure
	Error error

	// Running is true when the volume snapshot is running or when we are
	// waiting for the external snapshotter operator to reconcile it
	Running bool
}

// VolumeSnapshotError is raised when a volume snapshot failed with
// an error
type VolumeSnapshotError struct {
	// InternalError is a representation of the error given
	// by the CSI driver
	InternalError storagesnapshotv1.VolumeSnapshotError

	// Name is the name of the VolumeSnapshot object
	Name string

	// Namespace is the namespace of the VolumeSnapshot object
	Namespace string
}

// Error implements the error interface
func (err VolumeSnapshotError) Error() string {
	if err.InternalError.Message != nil {
		return "non specified volume snapshot error"
	}
	return *err.InternalError.Message
}

// GetBackupVolumeSnapshots TODO
func GetBackupVolumeSnapshots(
	ctx context.Context,
	cli client.Client,
	namespace string,
	backupLabelName string,
) ([]storagesnapshotv1.VolumeSnapshot, error) {
	var list storagesnapshotv1.VolumeSnapshotList

	if err := cli.List(
		ctx,
		&list,
		client.InNamespace(namespace),
		client.MatchingLabels{utils.BackupNameLabelName: backupLabelName},
	); err != nil {
		return nil, err
	}

	return list.Items, nil
}

// GetTargetPodFromFencedInstances get the target Pod from the list of instances that are fenced.
// This is useful when a cold volume snapshot backup have been started and the target Pod
// is already been merged.
func GetTargetPodFromFencedInstances(
	ctx context.Context,
	c client.Client,
	cluster *apiv1.Cluster,
) (*corev1.Pod, error) {
	contextLogger := log.FromContext(ctx)

	// override pod to be the fenced instance if needed
	fencedPodNames, err := utils.GetFencedInstances(cluster.Annotations)
	if err != nil {
		// the fenced instances annotation have not the correct syntax
		return nil, err
	}

	if fencedPodNames.Len() != 1 {
		// We start a snapshot backup only when
		contextLogger.Info("Waiting for the target Pod to be the only one fenced Pod",
			"fencedPodNames", fencedPodNames.ToList())
		return nil, ErrUnexpectedFencedInstances
	}

	targetPodName := fencedPodNames.ToList()[0]
	var pod corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: targetPodName}, &pod); err != nil {
		return nil, fmt.Errorf("cannot get target Pod: %w", err)
	}

	return &pod, nil
}

// GetVolumeSnapshotInfo extracts information from a volume snapshot resource
func GetVolumeSnapshotInfo(snapshot *storagesnapshotv1.VolumeSnapshot) VolumeSnapshotInfo {
	if snapshot.Status == nil {
		return VolumeSnapshotInfo{
			Error:   nil,
			Running: true,
		}
	}

	if snapshot.Status.Error != nil {
		return VolumeSnapshotInfo{
			Error: &VolumeSnapshotError{
				InternalError: *snapshot.Status.Error,
				Name:          snapshot.Name,
				Namespace:     snapshot.Namespace,
			},
			Running: false,
		}
	}

	if snapshot.Status.ReadyToUse == nil || !*snapshot.Status.ReadyToUse {
		// This volume snapshot completed correctly
		return VolumeSnapshotInfo{
			Error:   nil,
			Running: true,
		}
	}

	return VolumeSnapshotInfo{
		Error:   nil,
		Running: false,
	}
}
