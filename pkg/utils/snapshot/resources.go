package snapshot

import (
	"context"

	storagesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
)

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
