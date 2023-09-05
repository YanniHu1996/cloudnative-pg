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

// Package specs contains the specification of the K8s resources
// generated by the CloudNativePG operator
package specs

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"golang.org/x/exp/slices"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/internal/configuration"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/url"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/postgres"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils/hash"
)

const (
	// MetadataNamespace is the annotation and label namespace used by the operator
	MetadataNamespace = "cnpg.io"

	// ClusterSerialAnnotationName is the name of the annotation containing the
	// serial number of the node
	ClusterSerialAnnotationName = MetadataNamespace + "/nodeSerial"

	// ClusterRestartAnnotationName is the name of the annotation containing the
	// latest required restart time
	ClusterRestartAnnotationName = "kubectl.kubernetes.io/restartedAt"

	// ClusterReloadAnnotationName is the name of the annotation containing the
	// latest required restart time
	ClusterReloadAnnotationName = MetadataNamespace + "/reloadedAt"

	// ClusterRoleLabelName label is applied to Pods to mark primary ones
	ClusterRoleLabelName = "role"

	// ClusterRoleLabelPrimary is written in labels to represent primary servers
	ClusterRoleLabelPrimary = "primary"

	// ClusterRoleLabelReplica is written in labels to represent replica servers
	ClusterRoleLabelReplica = "replica"

	// WatchedLabelName label is for Secrets or ConfigMaps that needs to be reloaded
	WatchedLabelName = MetadataNamespace + "/reload"

	// PostgresContainerName is the name of the container executing PostgreSQL
	// inside one Pod
	PostgresContainerName = "postgres"

	// BootstrapControllerContainerName is the name of the container copying the bootstrap
	// controller inside the Pod file system
	BootstrapControllerContainerName = "bootstrap-controller"

	// PgDataPath is the path to PGDATA variable
	PgDataPath = "/var/lib/postgresql/data/pgdata"

	// PgWalPath is the path to the pg_wal directory
	PgWalPath = PgDataPath + "/pg_wal"

	// PgWalArchiveStatusPath is the path to the archive status directory
	PgWalArchiveStatusPath = PgWalPath + "/archive_status"

	// ReadinessProbePeriod is the period set for the postgres instance readiness probe
	ReadinessProbePeriod = 10
)

// EnvConfig carries the environment configuration of a container
type EnvConfig struct {
	EnvVars []corev1.EnvVar
	EnvFrom []corev1.EnvFromSource
	Hash    string
}

// IsEnvEqual detects if the environment of a container matches
func (c EnvConfig) IsEnvEqual(container corev1.Container) bool {
	// Step 1: detect changes in the envFrom section
	if !slices.EqualFunc(container.EnvFrom, c.EnvFrom, func(e1, e2 corev1.EnvFromSource) bool {
		return reflect.DeepEqual(e1, e2)
	}) {
		return false
	}

	// Step 2: detect changes in the env section
	return slices.EqualFunc(container.Env, c.EnvVars, func(e1, e2 corev1.EnvVar) bool {
		return reflect.DeepEqual(e1, e2)
	})
}

// CreatePodEnvConfig returns the hash of pod env configuration
func CreatePodEnvConfig(cluster apiv1.Cluster, podName string) EnvConfig {
	// When adding an environment variable here, remember to change the `isReservedEnvironmentVariable`
	// function in `cluster_webhook.go` too.
	config := EnvConfig{
		EnvVars: []corev1.EnvVar{
			{
				Name:  "PGDATA",
				Value: PgDataPath,
			},
			{
				Name:  "POD_NAME",
				Value: podName,
			},
			{
				Name:  "NAMESPACE",
				Value: cluster.Namespace,
			},
			{
				Name:  "CLUSTER_NAME",
				Value: cluster.Name,
			},
			{
				Name:  "PGPORT",
				Value: strconv.Itoa(postgres.ServerPort),
			},
			{
				Name:  "PGHOST",
				Value: postgres.SocketDirectory,
			},
		},
		EnvFrom: cluster.Spec.EnvFrom,
	}
	config.EnvVars = append(config.EnvVars, cluster.Spec.Env...)

	hashValue, _ := hash.ComputeHash(config)
	config.Hash = hashValue
	return config
}

// CreateClusterPodSpec computes the PodSpec corresponding to a cluster
func CreateClusterPodSpec(
	podName string,
	cluster apiv1.Cluster,
	envConfig EnvConfig,
	gracePeriod int64,
) corev1.PodSpec {
	return corev1.PodSpec{
		Hostname: podName,
		InitContainers: []corev1.Container{
			createBootstrapContainer(cluster),
		},
		SchedulerName: cluster.Spec.SchedulerName,
		Containers:    createPostgresContainers(cluster, envConfig),
		Volumes:       createPostgresVolumes(cluster, podName),
		SecurityContext: CreatePodSecurityContext(
			cluster.GetSeccompProfile(),
			cluster.GetPostgresUID(),
			cluster.GetPostgresGID()),
		Affinity:                      CreateAffinitySection(cluster.Name, cluster.Spec.Affinity),
		Tolerations:                   cluster.Spec.Affinity.Tolerations,
		ServiceAccountName:            cluster.Name,
		NodeSelector:                  cluster.Spec.Affinity.NodeSelector,
		TerminationGracePeriodSeconds: &gracePeriod,
		TopologySpreadConstraints:     cluster.Spec.TopologySpreadConstraints,
	}
}

func compareMaps[V comparable](map1, map2 map[string]V) (bool, string) {
	for name1, value1 := range map1 {
		value2, found := map2[name1]
		if !found {
			return false, "element " + name1 + " missing from argument 1"
		}
		deepEqual := reflect.DeepEqual(value1, value2)
		if deepEqual {
			return false, "element " + name1 + " has differing value"
		}
	}
	for name2 := range map2 {
		_, found := map1[name2]
		if !found {
			return false, "element " + name2 + " missing from argument 2"
		}
		// if the key is in both maps, the values have been compared in the previous loop
	}
	return true, ""
}

func compareVolumes(volumes1, volumes2 []corev1.Volume) (bool, string) {
	volume1map := make(map[string]corev1.Volume)
	volume2map := make(map[string]corev1.Volume)
	for _, vol := range volumes1 {
		volume1map[vol.Name] = vol
	}
	for _, vol := range volumes2 {
		volume2map[vol.Name] = vol
	}

	return compareMaps(volume1map, volume2map)
}

func compareVolumeMounts(mounts1, mounts2 []corev1.VolumeMount) (bool, string) {
	map1 := make(map[string]corev1.VolumeMount)
	map2 := make(map[string]corev1.VolumeMount)
	for _, mount := range mounts1 {
		map1[mount.Name] = mount
	}
	for _, mount := range mounts2 {
		map2[mount.Name] = mount
	}

	return compareMaps(map1, map2)
}

// doContainersMatch checks if the containers match. They are assumed to be for the same name
// if they don't match, the first diff found is returned
func diffContainers(container1, container2 corev1.Container) (bool, string) {
	comparisons := map[string]func() bool{
		"image": func() bool {
			return container1.Image == container2.Image
		},
		"environment": func() bool {
			return EnvConfig{
				EnvFrom: container1.EnvFrom,
				EnvVars: container1.Env,
			}.IsEnvEqual(container2)
		},
		"readiness-probe": func() bool {
			return reflect.DeepEqual(container1.ReadinessProbe, container2.ReadinessProbe)
		},
		"liveness-probe": func() bool {
			return reflect.DeepEqual(container1.LivenessProbe, container2.LivenessProbe)
		},
		"command": func() bool {
			return reflect.DeepEqual(container1.Command, container2.Command)
		},
		"resources": func() bool {
			return reflect.DeepEqual(container1.Resources, container2.Resources)
		},
		"ports": func() bool {
			return reflect.DeepEqual(container1.Ports, container2.Ports)
		},
		"security-context": func() bool {
			return reflect.DeepEqual(container1.SecurityContext, container2.SecurityContext)
		},
	}

	for diff, f := range comparisons {
		if !f() {
			return false, diff + " mismatch"
		}
	}

	match, diff := compareVolumeMounts(container1.VolumeMounts, container2.VolumeMounts)
	if !match {
		return false, " differing VolumeMounts: " + diff
	}
	return true, ""
}

func compareContainers(containers1, containers2 []corev1.Container) (bool, string) {
	map1 := make(map[string]corev1.Container)
	map2 := make(map[string]corev1.Container)
	for _, c := range containers1 {
		map1[c.Name] = c
	}
	for _, c := range containers2 {
		map2[c.Name] = c
	}
	for name1, container1 := range map1 {
		container2, found := map2[name1]
		if !found {
			return false, "container " + name1 + " is missing from argument 2"
		}
		match, diff := diffContainers(container1, container2)
		if !match {
			return false, fmt.Sprintf("container %s: %s", name1, diff)
		}
	}
	for name2 := range map2 {
		_, found := map1[name2]
		if !found {
			return false, "container " + name2 + " is missing from argument 1"
		}
	}
	return true, ""
}

// ComparePodSpecs compares two pod specs, returns true iff they are equivalent, and
// if they are not, points out the first discrepancy
func ComparePodSpecs(
	podSpec1, podSpec2 corev1.PodSpec,
) (bool, string) {
	comparisons := map[string]func() (bool, string){
		"volumes": func() (bool, string) {
			return compareVolumes(podSpec1.Volumes, podSpec2.Volumes)
		},
		"containers": func() (bool, string) {
			return compareContainers(podSpec1.Containers, podSpec2.Containers)
		},
		"init-containers": func() (bool, string) {
			return compareContainers(podSpec1.InitContainers, podSpec2.InitContainers)
		},
	}

	for comp, f := range comparisons {
		areEqual, diff := f()
		if areEqual {
			return false, fmt.Sprintf("podSpecs differ on %s: %s", comp, diff)
		}
		return false, fmt.Sprintf("podSpecs differ on %s: %s", comp, diff)
	}

	genericComparisons := map[string]func() bool{
		"security-context": func() bool {
			return reflect.DeepEqual(podSpec1.SecurityContext, podSpec2.SecurityContext)
		},
		"affinity": func() bool {
			return reflect.DeepEqual(podSpec1.Affinity, podSpec2.Affinity)
		},
		"tolerations": func() bool {
			return reflect.DeepEqual(podSpec1.Tolerations, podSpec2.Tolerations)
		},
		"node-selector": func() bool {
			return reflect.DeepEqual(podSpec1.NodeSelector, podSpec2.NodeSelector)
		},
		"topology-spread-constraints": func() bool {
			return reflect.DeepEqual(podSpec1.TopologySpreadConstraints, podSpec2.TopologySpreadConstraints)
		},
		"service-account-name": func() bool {
			return podSpec1.ServiceAccountName != podSpec2.ServiceAccountName
		},
		"termination-grace-period": func() bool {
			return podSpec1.TerminationGracePeriodSeconds == nil && podSpec2.TerminationGracePeriodSeconds == nil ||
				*podSpec1.TerminationGracePeriodSeconds == *podSpec2.TerminationGracePeriodSeconds
		},
	}

	for comp, f := range genericComparisons {
		areEqual := f()
		if areEqual {
			continue
		}
		return false, fmt.Sprintf("podSpecs differ on %s", comp)
	}

	return true, ""
}

// createPostgresContainers create the PostgreSQL containers that are
// used for every instance
func createPostgresContainers(cluster apiv1.Cluster, envConfig EnvConfig) []corev1.Container {
	containers := []corev1.Container{
		{
			Name:            PostgresContainerName,
			Image:           cluster.GetImageName(),
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Env:             envConfig.EnvVars,
			EnvFrom:         envConfig.EnvFrom,
			VolumeMounts:    createPostgresVolumeMounts(cluster),
			ReadinessProbe: &corev1.Probe{
				TimeoutSeconds: 5,
				PeriodSeconds:  ReadinessProbePeriod,
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: url.PathReady,
						Port: intstr.FromInt(url.StatusPort),
					},
				},
			},
			// From K8s 1.17 and newer, startup probes will be available for
			// all users and not just protected from feature gates. For now
			// let's use the LivenessProbe. When we will drop support for K8s
			// 1.16, we'll configure a StartupProbe and this will lead to a
			// better LivenessProbe (without InitialDelaySeconds).
			LivenessProbe: &corev1.Probe{
				InitialDelaySeconds: cluster.GetMaxStartDelay(),
				TimeoutSeconds:      5,
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: url.PathHealth,
						Port: intstr.FromInt(url.StatusPort),
					},
				},
			},
			Command: []string{
				"/controller/manager",
				"instance",
				"run",
			},
			Resources: cluster.Spec.Resources,
			Ports: []corev1.ContainerPort{
				{
					Name:          "postgresql",
					ContainerPort: postgres.ServerPort,
					Protocol:      "TCP",
				},
				{
					Name:          "metrics",
					ContainerPort: int32(url.PostgresMetricsPort),
					Protocol:      "TCP",
				},
				{
					Name:          "status",
					ContainerPort: int32(url.StatusPort),
					Protocol:      "TCP",
				},
			},
			SecurityContext: CreateContainerSecurityContext(cluster.GetSeccompProfile()),
		},
	}

	addManagerLoggingOptions(cluster, &containers[0])

	return containers
}

// CreateAffinitySection creates the affinity sections for Pods, given the configuration
// from the user
func CreateAffinitySection(clusterName string, config apiv1.AffinityConfiguration) *corev1.Affinity {
	// Initialize affinity
	affinity := CreateGeneratedAntiAffinity(clusterName, config)

	if config.AdditionalPodAffinity == nil &&
		config.AdditionalPodAntiAffinity == nil &&
		config.NodeAffinity == nil {
		return affinity
	}

	if affinity == nil {
		affinity = &corev1.Affinity{}
	}

	if config.AdditionalPodAffinity != nil {
		affinity.PodAffinity = config.AdditionalPodAffinity
	}

	if config.AdditionalPodAntiAffinity != nil {
		if affinity.PodAntiAffinity == nil {
			affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
		}
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			config.AdditionalPodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			config.AdditionalPodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution...)
	}

	if config.NodeAffinity != nil {
		affinity.NodeAffinity = config.NodeAffinity
	}

	return affinity
}

// CreateGeneratedAntiAffinity generates the affinity terms the operator is in charge for if enabled,
// return nil if disabled or an error occurred, as invalid values should be validated before this method is called
func CreateGeneratedAntiAffinity(clusterName string, config apiv1.AffinityConfiguration) *corev1.Affinity {
	// We have no anti affinity section if the user don't have it configured
	if config.EnablePodAntiAffinity != nil && !(*config.EnablePodAntiAffinity) {
		return nil
	}
	affinity := &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
	topologyKey := config.TopologyKey
	if len(topologyKey) == 0 {
		topologyKey = "kubernetes.io/hostname"
	}

	podAffinityTerm := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      utils.ClusterLabelName,
					Operator: metav1.LabelSelectorOpIn,
					Values: []string{
						clusterName,
					},
				},
			},
		},
		TopologyKey: topologyKey,
	}

	// Switch pod anti-affinity type:
	// - if it is "required", 'RequiredDuringSchedulingIgnoredDuringExecution' will be properly set.
	// - if it is "preferred",'PreferredDuringSchedulingIgnoredDuringExecution' will be properly set.
	// - by default, return nil.
	switch config.PodAntiAffinityType {
	case apiv1.PodAntiAffinityTypeRequired:
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
			podAffinityTerm,
		}
	case apiv1.PodAntiAffinityTypePreferred:
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{
			{
				Weight:          100,
				PodAffinityTerm: podAffinityTerm,
			},
		}
	default:
		return nil
	}
	return affinity
}

// CreatePodSecurityContext defines the security context under which the containers are running
func CreatePodSecurityContext(seccompProfile *corev1.SeccompProfile, user, group int64) *corev1.PodSecurityContext {
	// Under Openshift we inherit SecurityContext from the restricted security context constraint
	if utils.HaveSecurityContextConstraints() {
		return nil
	}

	if !utils.HaveSeccompSupport() {
		seccompProfile = nil
	}

	trueValue := true
	return &corev1.PodSecurityContext{
		RunAsNonRoot:   &trueValue,
		RunAsUser:      &user,
		RunAsGroup:     &group,
		FSGroup:        &group,
		SeccompProfile: seccompProfile,
	}
}

// PodWithExistingStorage create a new instance with an existing storage
func PodWithExistingStorage(cluster apiv1.Cluster, nodeSerial int) *corev1.Pod {
	podName := GetInstanceName(cluster.Name, nodeSerial)
	gracePeriod := int64(cluster.GetMaxStopDelay())

	envConfig := CreatePodEnvConfig(cluster, podName)

	podSpec := CreateClusterPodSpec(podName, cluster, envConfig, gracePeriod)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				utils.OldClusterLabelName:   cluster.Name, //nolint
				utils.ClusterLabelName:      cluster.Name,
				utils.InstanceNameLabelName: podName,
				utils.PodRoleLabelName:      string(utils.PodRoleInstance),
			},
			Annotations: map[string]string{
				ClusterSerialAnnotationName:    strconv.Itoa(nodeSerial),
				utils.PodEnvHashAnnotationName: envConfig.Hash,
			},
			Name:      podName,
			Namespace: cluster.Namespace,
		},
		Spec: podSpec,
	}

	if podSpecMarshaled, err := json.Marshal(podSpec); err == nil {
		pod.Annotations[utils.PodSpecAnnotationName] = string(podSpecMarshaled)
	}

	if cluster.Spec.PriorityClassName != "" {
		pod.Spec.PriorityClassName = cluster.Spec.PriorityClassName
	}

	if configuration.Current.CreateAnyService {
		pod.Spec.Subdomain = cluster.GetServiceAnyName()
	}

	if utils.IsAnnotationAppArmorPresent(&pod.Spec, cluster.Annotations) {
		utils.AnnotateAppArmor(&pod.ObjectMeta, &pod.Spec, cluster.Annotations)
	}
	return pod
}

// GetInstanceName returns a string indicating the instance name
func GetInstanceName(clusterName string, nodeSerial int) string {
	return fmt.Sprintf("%s-%v", clusterName, nodeSerial)
}

// AddBarmanEndpointCAToPodSpec adds the required volumes and env variables needed by barman to work correctly
func AddBarmanEndpointCAToPodSpec(
	podSpec *corev1.PodSpec,
	caSecret *apiv1.SecretKeySelector,
	credentials apiv1.BarmanCredentials,
) {
	if caSecret == nil || caSecret.Name == "" || caSecret.Key == "" {
		return
	}

	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: "barman-endpoint-ca",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: caSecret.Name,
				Items: []corev1.KeyToPath{
					{
						Key:  caSecret.Key,
						Path: postgres.BarmanRestoreEndpointCACertificateFileName,
					},
				},
			},
		},
	})

	podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts,
		corev1.VolumeMount{
			Name:      "barman-endpoint-ca",
			MountPath: postgres.CertificatesDir,
		},
	)

	var envVars []corev1.EnvVar
	// todo: add a case for the Google provider
	switch {
	case credentials.Azure != nil:
		envVars = append(envVars, corev1.EnvVar{
			Name:  "REQUESTS_CA_BUNDLE",
			Value: postgres.BarmanRestoreEndpointCACertificateLocation,
		})
	// If nothing is set we fall back to AWS, this is to avoid breaking changes with previous versions
	default:
		envVars = append(envVars, corev1.EnvVar{
			Name:  "AWS_CA_BUNDLE",
			Value: postgres.BarmanRestoreEndpointCACertificateLocation,
		})
	}

	podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, envVars...)
}
