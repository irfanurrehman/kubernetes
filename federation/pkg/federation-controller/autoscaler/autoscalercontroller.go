/*
Copyright 2016 The Kubernetes Authors.

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

package autoscaler

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	clientv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	fedapi "k8s.io/kubernetes/federation/apis/federation"
	fedv1 "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	fedclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_clientset"
	fedutil "k8s.io/kubernetes/federation/pkg/federation-controller/util"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/deletionhelper"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/eventsink"
	"k8s.io/kubernetes/pkg/api"
	autoscalingv1 "k8s.io/kubernetes/pkg/apis/autoscaling/v1"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/golang/glog"
)

const (
	FedHpaPreferencesAnnotation = "federation.kubernetes.io/hpa-preferences"
	allClustersKey              = "THE_ALL_CLUSTER_KEY"
	scaleForbiddenWindow        = 5 * time.Minute
)

type AutoscalerController struct {
	fedClient fedclientset.Interface

	autoscalerController cache.Controller
	autoscalerStore      cache.Store

	fedInformer fedutil.FederatedInformer

	autoscalerDeliverer *fedutil.DelayingDeliverer
	clusterDeliverer    *fedutil.DelayingDeliverer

	// For updating members of federation.
	fedUpdater fedutil.FederatedUpdater

	autoscalerBackoff *flowcontrol.Backoff
	// For events
	eventRecorder record.EventRecorder

	deletionHelper *deletionhelper.DeletionHelper

	autoscalerReviewDelay time.Duration
	clusterAvailableDelay time.Duration
	updateTimeout         time.Duration
}

// NewclusterController returns a new cluster controller
func NewAutoscalerController(fedClient fedclientset.Interface) *AutoscalerController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(eventsink.NewFederatedEventSink(fedClient))
	recorder := broadcaster.NewRecorder(api.Scheme, clientv1.EventSource{Component: "federated-autoscaler-controller"})

	fac := &AutoscalerController{
		fedClient:             fedClient,
		autoscalerDeliverer:   fedutil.NewDelayingDeliverer(),
		clusterDeliverer:      fedutil.NewDelayingDeliverer(),
		autoscalerBackoff:     flowcontrol.NewBackOff(5*time.Second, 1*time.Minute),
		eventRecorder:         recorder,
		autoscalerReviewDelay: 10 * time.Second,
		clusterAvailableDelay: 20 * time.Second,
		updateTimeout:         30 * time.Second,
	}

	fac.autoscalerStore, fac.autoscalerController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return fac.fedClient.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceAll).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return fac.fedClient.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceAll).Watch(options)
			},
		},
		&autoscalingv1.HorizontalPodAutoscaler{},
		controller.NoResyncPeriodFunc(),
		fedutil.NewTriggerOnAllChanges(
			func(obj runtime.Object) { fac.deliverAutoscalerObj(obj, 0, false) },
		),
	)

	// Federated informer on autoscalers in members of federation.
	fac.fedInformer = fedutil.NewFederatedInformer(
		fedClient,
		func(cluster *fedv1.Cluster, targetClient kubeclientset.Interface) (cache.Store, cache.Controller) {
			return cache.NewInformer(
				&cache.ListWatch{
					ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
						return targetClient.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceAll).List(options)
					},
					WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
						return targetClient.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceAll).Watch(options)
					},
				},
				&autoscalingv1.HorizontalPodAutoscaler{},
				controller.NoResyncPeriodFunc(),
				// Trigger reconciliation whenever something in federated cluster is changed. In most cases it
				// would be just confirmation that some autoscaler update succeeded.
				fedutil.NewTriggerOnMetaAndSpecChanges(
					func(obj runtime.Object) { fac.deliverAutoscalerObj(obj, fac.autoscalerReviewDelay, false) },
				))
		},
		&fedutil.ClusterLifecycleHandlerFuncs{
			ClusterAvailable: func(cluster *fedv1.Cluster) {
				// When new cluster becomes available process all the autoscalers again.
				fac.clusterDeliverer.DeliverAfter(allClustersKey, nil, fac.clusterAvailableDelay)
			},
		},
	)

	fac.fedUpdater = fedutil.NewFederatedUpdater(fac.fedInformer,
		func(client kubeclientset.Interface, obj runtime.Object) error {
			hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
			_, err := client.AutoscalingV1().HorizontalPodAutoscalers(hpa.Namespace).Create(hpa)
			return err
		},
		func(client kubeclientset.Interface, obj runtime.Object) error {
			hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
			_, err := client.AutoscalingV1().HorizontalPodAutoscalers(hpa.Namespace).Update(hpa)
			return err
		},
		func(client kubeclientset.Interface, obj runtime.Object) error {
			hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
			err := client.AutoscalingV1().HorizontalPodAutoscalers(hpa.Namespace).Delete(hpa.Name, &metav1.DeleteOptions{})
			return err
		})

	fac.deletionHelper = deletionhelper.NewDeletionHelper(
		fac.hasFinalizerFunc,
		fac.removeFinalizerFunc,
		fac.addFinalizerFunc,
		// objNameFunc
		func(obj runtime.Object) string {
			hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
			return hpa.Name
		},
		fac.updateTimeout,
		fac.eventRecorder,
		fac.fedInformer,
		fac.fedUpdater,
	)
	return fac
}

// Returns true if the given object has the given finalizer in its ObjectMeta.
func (fac *AutoscalerController) hasFinalizerFunc(obj runtime.Object, finalizer string) bool {
	hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	for i := range hpa.ObjectMeta.Finalizers {
		if string(hpa.ObjectMeta.Finalizers[i]) == finalizer {
			return true
		}
	}
	return false
}

// Removes the finalizer from the given objects ObjectMeta.
// Assumes that the given object is a hpa.
func (fac *AutoscalerController) removeFinalizerFunc(obj runtime.Object, finalizers []string) (runtime.Object, error) {
	hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	newFinalizers := []string{}
	hasFinalizer := false
	for i := range hpa.ObjectMeta.Finalizers {
		if deletionhelper.ContainsString(finalizers, hpa.ObjectMeta.Finalizers[i]) {
			newFinalizers = append(newFinalizers, hpa.ObjectMeta.Finalizers[i])
		} else {
			hasFinalizer = true
		}
	}
	if !hasFinalizer {
		// Nothing to do.
		return obj, nil
	}
	hpa.ObjectMeta.Finalizers = newFinalizers
	hpa, err := fac.fedClient.AutoscalingV1().HorizontalPodAutoscalers(hpa.Namespace).Update(hpa)
	if err != nil {
		return nil, fmt.Errorf("failed to remove finalizer %v from hpa %s: %v", finalizers, hpa.Name, err)
	}
	return hpa, nil
}

// Adds the given finalizer to the given objects ObjectMeta.
// Assumes that the given object is an hpa.
func (fac *AutoscalerController) addFinalizerFunc(obj runtime.Object, finalizers []string) (runtime.Object, error) {
	hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	hpa.ObjectMeta.Finalizers = append(hpa.ObjectMeta.Finalizers, finalizers...)
	hpa, err := fac.fedClient.AutoscalingV1().HorizontalPodAutoscalers(hpa.Namespace).Update(hpa)
	if err != nil {
		return nil, fmt.Errorf("failed to add finalizer %v to hpa %s: %v", finalizers, hpa.Name, err)
	}
	return hpa, nil
}

func (fac *AutoscalerController) Run(stopChan <-chan struct{}) {
	go fac.autoscalerController.Run(stopChan)
	fac.fedInformer.Start()
	go func() {
		<-stopChan
		fac.fedInformer.Stop()
	}()
	fac.autoscalerDeliverer.StartWithHandler(func(item *fedutil.DelayingDelivererItem) {
		hpa := item.Key
		fac.reconcileAutoscaler(hpa)
	})
	fac.clusterDeliverer.StartWithHandler(func(_ *fedutil.DelayingDelivererItem) {
		fac.reconcileAutoscalerOnClusterChange()
	})
	fedutil.StartBackoffGC(fac.autoscalerBackoff, stopChan)

	//TODO IRF: do we need to explicitly stop the deliverers at exit of this function
}

func (fac *AutoscalerController) isSynced() bool {
	if !fac.fedInformer.ClustersSynced() {
		glog.V(3).Infof("Cluster list not synced")
		return false
	}
	clusters, err := fac.fedInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get ready clusters: %v", err)
		return false
	}
	if !fac.fedInformer.GetTargetStore().ClustersSynced(clusters) {
		glog.V(2).Infof("cluster hpa list not synced")
		return false
	}

	if !fac.autoscalerController.HasSynced() {
		glog.V(2).Infof("federation hpa list not synced")
		return false
	}
	return true
}

func (fac *AutoscalerController) deliverAutoscalerObj(obj interface{}, delay time.Duration, failed bool) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		glog.Errorf("Couldn't get key for object %+v: %v", obj, err)
		return
	}

	fac.deliverAutoscaler(key, delay, failed)
}

// Adds backoff to delay if this delivery is related to some failure. Resets backoff if there was no failure.
func (fac *AutoscalerController) deliverAutoscaler(hpa string, delay time.Duration, failed bool) {
	if failed {
		fac.autoscalerBackoff.Next(hpa, time.Now())
		delay = delay + fac.autoscalerBackoff.Get(hpa)
	} else {
		fac.autoscalerBackoff.Reset(hpa)
	}
	fac.autoscalerDeliverer.DeliverAfter(hpa, nil, delay)
}

//func (fac *AutoscalerController) reconcileAutoscaler(key string) (reconciliationStatus, error)
func (fac *AutoscalerController) reconcileAutoscaler(hpa string) {
	if !fac.isSynced() {
		fac.deliverAutoscaler(hpa, fac.clusterAvailableDelay, false)
		return
	}

	ObjFromStore, exist, err := fac.autoscalerStore.GetByKey(hpa)
	if err != nil {
		glog.Errorf("Failed to query main hpa store for %v: %v", hpa, err)
		fac.deliverAutoscaler(hpa, 0, true)
		return
	}

	if !exist {
		// Not federated hpa, ignoring.
		return
	}
	// Create a copy before modifying the hpa to prevent race condition with
	// other readers of hpa from store.
	copiedObj, err := api.Scheme.DeepCopy(ObjFromStore)
	fedHpa, ok := copiedObj.(*autoscalingv1.HorizontalPodAutoscaler)
	if err != nil || !ok {
		glog.Errorf("Error in retrieving obj from store: %v, %v", ok, err)
		fac.deliverAutoscaler(hpa, 0, true)
		return
	}

	if fedHpa.DeletionTimestamp != nil {
		if err := fac.delete(fedHpa); err != nil {
			glog.Errorf("Failed to delete %s: %v", hpa, err)
			fac.eventRecorder.Eventf(fedHpa, api.EventTypeNormal, "DeleteFailed",
				"HPA delete failed: %v", err)
			fac.deliverAutoscaler(hpa, 0, true)
		}
		return
	}

	glog.V(3).Infof("Ensuring delete object from underlying clusters finalizer for hpa: %s",
		fedHpa.Name)
	// Add the required finalizers before creating an hpa in
	// underlying clusters.
	// This ensures that the dependent hpas are deleted in underlying
	// clusters when the federated hpa is deleted.
	updatedHpaObj, err := fac.deletionHelper.EnsureFinalizers(fedHpa)
	if err != nil {
		glog.Errorf("Failed to ensure delete object from underlying clusters finalizer for hpa %s: %v",
			fedHpa.Name, err)
		fac.deliverAutoscaler(hpa, 0, false)
		return
	}
	fedHpa = updatedHpaObj.(*autoscalingv1.HorizontalPodAutoscaler)

	glog.V(3).Infof("Syncing hpa %s in underlying clusters", fedHpa.Name)
	// Sync the hpa in all underlying clusters.
	clusters, err := fac.fedInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get cluster list: %v", err)
		fac.deliverAutoscaler(hpa, fac.clusterAvailableDelay, false)
		return
	}

	current := make(map[string]runtime.Object)
	for _, cluster := range clusters {
		clusterHpaObj, found, err := fac.fedInformer.GetTargetStore().GetByKey(cluster.Name, hpa)
		if err != nil {
			glog.Errorf("Failed to get %s from %s: %v", hpa, cluster.Name, err)
			fac.deliverAutoscaler(hpa, 0, true)
			return
		}

		if !found {
			current[cluster.Name] = nil
		} else {
			current[cluster.Name] = clusterHpaObj.(*autoscalingv1.HorizontalPodAutoscaler)
		}
	}

	planned := scheduleHpa(fedHpa, current)
	operations := make([]fedutil.FederatedOperation, 0)
	for cluster, obj := range current {
		if obj != nil && planned[cluster] == nil {
			// delete the local hpa
			fac.eventRecorder.Eventf(fedHpa, api.EventTypeWarning, "DeleteFromCluster",
				"deleting hpa from cluster %s", cluster)
			fmt.Printf("\nDeleteFromCluster deleting hpa from cluster %s\n", cluster)
			operations = append(operations, fedutil.FederatedOperation{
				Type:        fedutil.OperationTypeDelete,
				Obj:         current[cluster],
				ClusterName: cluster,
			})
		} else if obj == nil && planned[cluster] != nil {
			// create a new local hpa
			fac.eventRecorder.Eventf(fedHpa, api.EventTypeNormal, "CreateInCluster",
				"Creating hpa in cluster %s", cluster)
			fmt.Printf("\nCreateInCluster, Creating hpa in cluster %s - HPA = %v \n", cluster, planned[cluster])
			operations = append(operations, fedutil.FederatedOperation{
				Type:        fedutil.OperationTypeAdd,
				Obj:         planned[cluster],
				ClusterName: cluster,
			})
		} else if obj != nil && planned[cluster] != nil {
			// modify the local hpa
			// If spec changes, it also means we overwrite the whole status
			// TODO: take care of status updates accordingly separately
			if !fedutil.ObjectMetaAndSpecEquivalent(planned[cluster], current[cluster]) {
				fac.eventRecorder.Eventf(fedHpa, api.EventTypeNormal, "UpdateInCluster",
					"Updating hpa in cluster %s. Desired: %+v\n Actual: %+v\n", cluster, planned[cluster], current[cluster])
				fmt.Printf("\nUpdating hpa in cluster %s. Desired: %+v\n Actual: %+v\n", cluster, planned[cluster], current[cluster])
				operations = append(operations, fedutil.FederatedOperation{
					Type:        fedutil.OperationTypeUpdate,
					Obj:         planned[cluster],
					ClusterName: cluster,
				})
			}
		}
	}

	/*for _, cluster := range clusters {
		clusterAutoscalerObj, found, err := fac.fedInformer.GetTargetStore().GetByKey(cluster.Name, hpa)
		if err != nil {
			glog.Errorf("Failed to get %s from %s: %v", hpa, cluster.Name, err)
			fac.deliverAutoscaler(hpa, 0, true)
			return
		}
		// The object should not be modified.
		desiredAutoscaler := &autoscalingv1.HorizontalPodAutoscaler{
			ObjectMeta: fedutil.DeepCopyRelevantObjectMeta(fedHpa.ObjectMeta),
			Spec:       fedutil.DeepCopyApiTypeOrPanic(fedHpa.Spec).(autoscalingv1.HorizontalPodAutoscalerSpec),
		}
		glog.V(5).Infof("Desired hpa in underlying clusters: %+v", desiredAutoscaler)

		if !found {
			fac.eventRecorder.Eventf(fedHpa, api.EventTypeNormal, "CreateInCluster",
				"Creating hpa in cluster %s", cluster.Name)

			operations = append(operations, fedutil.FederatedOperation{
				Type:        fedutil.OperationTypeAdd,
				Obj:         desiredAutoscaler,
				ClusterName: cluster.Name,
			})
		} else {
			clusterAutoscaler := clusterAutoscalerObj.(*autoscalingv1.HorizontalPodAutoscaler)

			// Update existing hpa, if needed.
			if !fedutil.ObjectMetaAndSpecEquivalent(desiredAutoscaler, clusterAutoscaler) {
				fac.eventRecorder.Eventf(fedHpa, api.EventTypeNormal, "UpdateInCluster",
					"Updating hpa in cluster %s. Desired: %+v\n Actual: %+v\n", cluster.Name, desiredAutoscaler, clusterAutoscaler)

				operations = append(operations, fedutil.FederatedOperation{
					Type:        fedutil.OperationTypeUpdate,
					Obj:         desiredAutoscaler,
					ClusterName: cluster.Name,
				})
			}
		}
	}*/

	if toUpdateObj, needUpdate := updateSpec(fedHpa, current); needUpdate {
		toUpdateHpa := toUpdateObj.(*autoscalingv1.HorizontalPodAutoscaler)
		_, err = fac.fedClient.AutoscalingV1().HorizontalPodAutoscalers(fedHpa.Namespace).UpdateStatus(toUpdateHpa)
		if err != nil {
			glog.Errorf("Failed to update the status of federation HPA %s: %v",
				fedHpa.Name, err)
			fac.deliverAutoscaler(hpa, 0, false)
			return
		}
	}

	if len(operations) == 0 {
		// Everything is in order
		return
	}
	glog.V(2).Infof("Updating hpa %s in underlying clusters. Operations: %d", fedHpa.Name, len(operations))

	err = fac.fedUpdater.UpdateWithOnError(operations, fac.updateTimeout, func(op fedutil.FederatedOperation, operror error) {
		fac.eventRecorder.Eventf(fedHpa, api.EventTypeNormal, "UpdateInClusterFailed",
			"HPA update in cluster %s failed: %v", op.ClusterName, operror)
		fmt.Println("UpdateInClusterFailed : HPA update in cluster %s failed: %v", op.ClusterName, operror)
	})
	if err != nil {
		glog.Errorf("Failed to execute updates for %s: %v", hpa, err)
		fmt.Println("Failed to execute updates for %s: %v", hpa, err)
		fac.deliverAutoscaler(hpa, 0, true)
		return
	}

	// Evertyhing is in order but lets be double sure
	fac.deliverAutoscaler(hpa, fac.autoscalerReviewDelay, false)
}

func (fac *AutoscalerController) reconcileAutoscalerOnClusterChange() {
	if !fac.isSynced() {
		fac.clusterDeliverer.DeliverAfter(allClustersKey, nil, fac.clusterAvailableDelay)
	}

	for _, obj := range fac.autoscalerStore.List() {
		hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
		fac.deliverAutoscaler(hpa.Name, time.Second*3, false)
	}
}

// delete deletes the given hpa or returns error if the deletion was not complete.
func (fac *AutoscalerController) delete(hpa *autoscalingv1.HorizontalPodAutoscaler) error {
	glog.V(3).Infof("Handling deletion of hpa: %s/%s\n", hpa.Namespace, hpa.Name)
	_, err := fac.deletionHelper.HandleObjectInUnderlyingClusters(hpa)
	if err != nil {
		return err
	}

	err = fac.fedClient.AutoscalingV1().HorizontalPodAutoscalers(hpa.Namespace).Delete(hpa.Name, nil)
	if err != nil {
		// Its all good if the error type is not found. That means it is deleted already and we do not have to do anything.
		// This is expected when we are processing an update as a result of hpa finalizer deletion.
		// The process that deleted the last finalizer is also going to delete the hpa and we do not have to do anything.
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete hpa: %s/%s, %v\n", hpa.Namespace, hpa.Name, err)
		}
	}
	return nil
}

type namedPerClusterPreferences struct {
	clusterName string
	hash        uint32
	fedapi.PerClusterPreferences
}

type localHpaSpec struct {
	min int32
	max int32
}

type byWeight []*namedPerClusterPreferences

func (a byWeight) Len() int      { return len(a) }
func (a byWeight) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Preferences are sorted according by decreasing weight and increasing hash (built on top of cluster name and rs name).
// Sorting is made by a hash to avoid assigning single-replica rs to the alphabetically smallest cluster.
func (a byWeight) Less(i, j int) bool {
	return (a[i].Weight > a[j].Weight) || (a[i].Weight == a[j].Weight && a[i].hash < a[j].hash)
}

func scheduleHpa(obj runtime.Object, current map[string]runtime.Object) map[string]runtime.Object {

	// rPoolMin // redistribution pool min
	// rPoolMax // redistribution pool max

	// // Sort the incoming list of hpa's by metric consumption highest consumption on top
	// Fairly randomise the incoming list // use map

	//func (p *Planner) Plan(replicasToDistribute int64, availableClusters []string, currentReplicaCount map[string]int64,
	//	estimatedCapacity map[string]int64, replicaSetKey string) (map[string]int64, map[string]int64) {

	fedHpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)

	var requestedMax int32 = fedHpa.Spec.MaxReplicas
	var requestedMin int32 = 1
	if fedHpa.Spec.MinReplicas != nil {
		requestedMin = *fedHpa.Spec.MinReplicas
	}

	fmt.Println("\nAT TOP: fedhpa\n", fedHpa)

	// These hold the list of those clusters which can offer
	// min and max replicas, to those which want them.
	// For example new clusters joining the federation and/or
	// those clusters which need to increase or reduce replicas
	// beyond min/max limits.
	//availableMaxList := []string{}
	//availableMinList := []string{}

	// replica distribution count per cluster
	rdcMin := requestedMin / int32(len(current))
	if rdcMin < 0 {
		rdcMin = 1
	}
	// TODO: Is there a better way
	// We need to cap the lowest limit of Max to 2, because in a
	// situation like both min and max become 1 (same) for all clusters,
	// no rebalancing would happen.
	rdcMax := requestedMax / int32(len(current))
	if rdcMax < 2 {
		rdcMax = 2
	}

	type schedMinMax struct {
		min int32
		max int32
	}
	scheduled := make(map[string]*schedMinMax)
	availableMaxList := sets.NewString()
	availableMinList := sets.NewString()
	noHpaList := sets.NewString()
	var existingMin int32 = 0
	var existingMax int32 = 0

	// First pass: Analyse existing local hpa's if any.
	// current has the list of all clusters, with obj as nil
	// for those clusters which do not have hpa yet.
	for cluster, obj := range current {
		fmt.Println("\n PASS 1: Iterating current:\n", cluster, obj)
		if obj == nil {
			fmt.Println("got nil current obj", cluster)
			noHpaList.Insert(cluster)
			scheduled[cluster] = nil
			continue
		}
		if maxReplicasReducible(obj) {
			availableMaxList.Insert(cluster)
		}
		if minReplicasReducible(obj) {
			availableMinList.Insert(cluster)
		}

		// TODO: Do we need to make a copy of this object
		// Probably copy it out only when needed in next pass
		scheduled[cluster] = &schedMinMax{min: 0, max: 0}
		if obj.(*autoscalingv1.HorizontalPodAutoscaler).Spec.MinReplicas != nil {
			existingMin += *obj.(*autoscalingv1.HorizontalPodAutoscaler).Spec.MinReplicas
			scheduled[cluster].min += *obj.(*autoscalingv1.HorizontalPodAutoscaler).Spec.MinReplicas
		}
		existingMax += obj.(*autoscalingv1.HorizontalPodAutoscaler).Spec.MaxReplicas
		scheduled[cluster].max += obj.(*autoscalingv1.HorizontalPodAutoscaler).Spec.MaxReplicas
		fmt.Println("PASS 1: updated scheduled", cluster, scheduled[cluster])

	}

	var remainingMin int32 = requestedMin - existingMin
	var remainingMax int32 = requestedMax - existingMax
	fmt.Println("remaining:", remainingMin, remainingMax)

	// Second pass: reduction of replicas across clusters if needed.
	// In this pass, we remain pessimistic and reduce one replica per cluster at a time.

	// The requested fedHpa has requested/updated min max to
	// something lesser then current local hpas.
	if remainingMin < 0 {
		// first we try reducing from those clusters which already offer min
		if availableMinList.Len() > 0 {
			for _, cluster := range availableMinList.List() {
				if (scheduled[cluster].min - 1) != 0 {
					scheduled[cluster].min--
					availableMinList.Delete(cluster)
					remainingMin++
					if remainingMin == 0 {
						break
					}
				}
			}
		}
	}
	// If we could not get needed replicas from already offered min above
	// we abruptly start removing replicas from some/all clusters.
	// Here we might make some min to 0 signalling that this hpa be
	// removed from this cluster altogether
	for remainingMin < 0 {
		for cluster := range scheduled {
			if scheduled[cluster] != nil &&
				!((scheduled[cluster].min - 1) < 0) {
				scheduled[cluster].min--
				remainingMin++
				if remainingMin == 0 {
					break
				}
			}
		}
	}

	if remainingMax < 0 {
		// first we try reducing from those clusters which already offer max
		if availableMaxList.Len() > 0 {
			for _, cluster := range availableMaxList.List() {
				if !((scheduled[cluster].max - scheduled[cluster].min) < 0) &&
					scheduled[cluster] != nil {
					scheduled[cluster].max--
					availableMaxList.Delete(cluster)
					remainingMax++
					if remainingMax == 0 {
						break
					}
				}
			}
		}
	}
	// If we could not get needed replicas to reduce from already offered
	// max above we abruptly start removing replicas from some/all clusters.
	// Here we might make some max and min to 0, signalling that this hpa be
	// removed from this cluster altogether
	for remainingMax < 0 {
		for cluster := range scheduled {
			if scheduled[cluster] != nil &&
				!((scheduled[cluster].max - scheduled[cluster].min) < 0) {
				scheduled[cluster].max--
				remainingMax++
				if remainingMax == 0 {
					break
				}
			}
		}
	}

	// Third pass: We distribute replicas into those clusters which need them.

	toDistributeMin := remainingMin + int32(availableMinList.Len())
	toDistributeMax := remainingMax + int32(availableMaxList.Len())

	fmt.Println("todistribute:", toDistributeMin, toDistributeMax)

	// Here we first distribute max and then distribute min
	// for the clusters which already get the upper limit fixed.
	// In this process we might not meet the min limit and
	// total of min limits might remain more then the requested
	// federated min. This is partially because a min per cluster
	// cannot be lesser then 1.
	// Additionally we first increase replicas into those clusters
	// which already has hpas and if we still have some remaining.
	// This will save cluster related resources for the user.
	// We then go ahead to give the replicas to those which do not
	// have any hpa. In this pass however we try to ensure that all
	// our Max are consumed in this reconcile.
	for cluster := range scheduled {
		fmt.Println("iterating scheduled:", cluster, scheduled[cluster])
		if toDistributeMax == 0 {
			break
		}
		if scheduled[cluster] == nil {
			continue
		}
		if maxReplicasNeeded(current[cluster]) {
			scheduled[cluster].max++
			if availableMaxList.Len() > 0 {
				popped, notEmpty := availableMaxList.PopAny()
				if notEmpty {
					// Boundary checks have happened earlier.
					// TODO: cross check again
					scheduled[popped].max--
				}
			}
			// Any which ways utilise available map replicas
			toDistributeMax--
		}
	}

	// If we do not have any new clusters where we can just
	// give all our replicas, then we stick to increase and
	// decrease of 1 replica and continue doing the same in
	// next reconcile cycles.
	if noHpaList.Len() > 0 {
		for toDistributeMax > 0 {
			for _, cluster := range noHpaList.UnsortedList() {
				if scheduled[cluster] == nil {
					scheduled[cluster] = &schedMinMax{min: 0, max: 0}
				}
				if toDistributeMax < rdcMax {
					scheduled[cluster].max += toDistributeMax
					toDistributeMax = 0
					break
				}
				scheduled[cluster].max += rdcMax
				toDistributeMax -= rdcMax
				fmt.Println("\n PASS 3: Clusters got max: distributed\n", cluster, scheduled[cluster])
			}
		}
	}

	// We distribute min to those clusters which:
	// 1 - can adjust min (our increase step would be only 1)
	// 2 - which do not have this hpa (increase step rdcMin)
	// Also after this distribution, we might still have some
	// minReplicas to distribute, but we ignore that in this reconcile.
	// On the other hand we might exaust all min replicas here, with
	// some clusters still needing them. We adjust this in next step by
	// assigning min replicas to 1 into those clusters which got max
	// but min remains 0.
	for cluster := range scheduled {
		if toDistributeMin == 0 {
			break
		}
		// We have distriubted Max and thus scheduled might not be nil
		// but probably current (what we got originally) is nil(no hpa)
		if scheduled[cluster] == nil || current[cluster] == nil {
			continue
		}
		if minReplicasAdjusteable(current[cluster]) {
			scheduled[cluster].min++
			if availableMaxList.Len() > 0 {
				popped, notEmpty := availableMinList.PopAny()
				if notEmpty {
					// Boundary checks have happened earlier.
					// TODO: cross check again
					scheduled[popped].min--
				}
			}
			toDistributeMin--
		}
	}

	if noHpaList.Len() > 0 {
		// TODO: can this really becomes an infinite loop?
		// Add a breaker anyways for starters.
		for pass := 0; toDistributeMin > 0 && pass < 100; pass++ {
			for _, cluster := range noHpaList.UnsortedList() {
				if scheduled[cluster] == nil {
					// We did not get max here so this cluster
					// remains without hpa
					continue
				}
				var replicaNum int32 = 0
				if toDistributeMin < rdcMin {
					replicaNum = toDistributeMin
				} else {
					replicaNum = rdcMin
				}
				if (scheduled[cluster].max - replicaNum) < scheduled[cluster].min {
					// Cannot increase the min in this cluster
					// as it will go beyond max
					continue
				}
				scheduled[cluster].min += replicaNum
				toDistributeMin -= replicaNum
			}
		}
	}

	planned := make(map[string]runtime.Object)

	// TODO: Optimisation: use schedule[string]runtime.object directly
	// in above code/computation to avoid this additional computation/copy.
	// But we would any way need to scan the list again for updating
	// min of those scheduled hpas where min <= 0 and max has some proper value.
	for cluster := range scheduled {
		if scheduled[cluster] == nil {
			planned[cluster] = nil
			continue
		}
		if (scheduled[cluster].min <= 0) && (scheduled[cluster].max > 0) {
			// Min total does not necessarily meet the federated min limit
			scheduled[cluster].min = 1
		}
		if scheduled[cluster].max <= 0 {
			// This ideally is a case were we should remove HPA from this cluser
			// but we dont, and follow the deletion helpers method.
			scheduled[cluster].max = 0
			scheduled[cluster].min = 0
		}
		if current[cluster] == nil {
			// This cluster got a new hpa
			planned[cluster] = getNewLocalHpa(fedHpa, scheduled[cluster].min, scheduled[cluster].max)
			fmt.Println("\nFINALISING:, cluster got new hpa\n", cluster, scheduled[cluster])
		} else {
			// we copy out the existing hpa from local cluster
			// and update the max and min if we need to, retaining the status.
			var lHpa *autoscalingv1.HorizontalPodAutoscaler
			lObj, err := api.Scheme.DeepCopy(current[cluster])
			if err != nil {
				// quite unlikely
				// TODO: is there some other way to recover from this error?
				lHpa = getNewLocalHpa(fedHpa, scheduled[cluster].min, scheduled[cluster].max)
			} else {
				lHpa = lObj.(*autoscalingv1.HorizontalPodAutoscaler)
			}
			if lHpa.Spec.MinReplicas == nil {
				var m int32 = scheduled[cluster].min
				lHpa.Spec.MinReplicas = &m
			} else {
				*lHpa.Spec.MinReplicas = scheduled[cluster].min
			}
			lHpa.Spec.MaxReplicas = scheduled[cluster].max
			planned[cluster] = lHpa
			fmt.Println("\nFINALISING:, copy the current object\n", current[cluster], lHpa)
		}
	}

	return planned
}

func getNewLocalHpa(fedHpa *autoscalingv1.HorizontalPodAutoscaler, min, max int32) *autoscalingv1.HorizontalPodAutoscaler {
	newHpa := autoscalingv1.HorizontalPodAutoscaler{
		ObjectMeta: fedutil.DeepCopyRelevantObjectMeta(fedHpa.ObjectMeta),
		Spec:       fedutil.DeepCopyApiTypeOrPanic(fedHpa.Spec).(autoscalingv1.HorizontalPodAutoscalerSpec),
	}
	if newHpa.Spec.MinReplicas == nil {
		var m int32 = min
		newHpa.Spec.MinReplicas = &m
	} else {
		*newHpa.Spec.MinReplicas = min
	}
	newHpa.Spec.MaxReplicas = max

	return &newHpa
}

// isScaleable tells if it already has been a subsequent amount of
// time since this hpa scaled. Its used to avoid fast thrashing.
func isScaleable(hpa *autoscalingv1.HorizontalPodAutoscaler) bool {
	if hpa.Status.LastScaleTime == nil {
		return false
	}
	t := hpa.Status.LastScaleTime.Add(scaleForbiddenWindow)
	if t.After(time.Now()) {
		return false
	}
	return true
}

func maxReplicasReducible(obj runtime.Object) bool {
	hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	// haven't scale in last 5 mins (or more)
	if !isScaleable(hpa) {
		return false
	}
	// max wont become less then min
	if (hpa.Spec.MinReplicas != nil) &&
		((hpa.Spec.MaxReplicas - *hpa.Spec.MinReplicas) < 0) {
		return false
	}
	if (hpa.Status.DesiredReplicas < hpa.Status.CurrentReplicas) ||
		((hpa.Status.DesiredReplicas == hpa.Status.CurrentReplicas) &&
			(hpa.Status.DesiredReplicas < hpa.Spec.MaxReplicas)) {
		return true
	}
	return false
}

// minReplicasReducible checks if this cluster (hpa) can offer replicas which are
// stuck here because of min limit.
// Its noteworthy, that min and max are adjusted separately, but if the replicas
// are not being used here, the max adjustment will lead it to become equal to min,
// but will not be able to scale down further and offer max to some other cluster
// which needs replicas.
func minReplicasReducible(obj runtime.Object) bool {
	hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	if !isScaleable(hpa) {
		return false
	}
	if (hpa.Spec.MinReplicas != nil) &&
		(*hpa.Spec.MinReplicas > 1) &&
		(*hpa.Spec.MinReplicas == hpa.Spec.MaxReplicas) {
		return true
	}
	// TODO: should we rather just check that max replica equals min and this check is useless
	if (hpa.Spec.MinReplicas != nil) &&
		(*hpa.Spec.MinReplicas > 1) &&
		(hpa.Status.DesiredReplicas == hpa.Status.CurrentReplicas) &&
		(hpa.Status.CurrentReplicas == *hpa.Spec.MinReplicas) {
		return true
	}
	return false
}

func maxReplicasNeeded(obj runtime.Object) bool {
	hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	// haven't scale in last 5 mins (or more)
	if !isScaleable(hpa) {
		return false
	}

	if (hpa.Status.CurrentReplicas == hpa.Status.DesiredReplicas) &&
		(hpa.Status.CurrentReplicas == hpa.Spec.MaxReplicas) {
		return true
	}
	return false
}

func minReplicasAdjusteable(obj runtime.Object) bool {
	hpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	// haven't scale in last 5 mins (or more)
	if !isScaleable(hpa) ||
		((hpa.Spec.MinReplicas != nil) &&
			(*hpa.Spec.MinReplicas+1) > hpa.Spec.MaxReplicas) {
		return false
	}

	if (hpa.Spec.MinReplicas != nil) &&
		(hpa.Status.DesiredReplicas > *hpa.Spec.MinReplicas) {
		return true
	}
	return false
}

func updateSpec(obj runtime.Object, current map[string]runtime.Object) (runtime.Object, bool) {
	fedHpa := obj.(*autoscalingv1.HorizontalPodAutoscaler)
	if len(current) == 0 {
		return nil, false
	}

	gen := fedHpa.Generation
	updatedStatus := autoscalingv1.HorizontalPodAutoscalerStatus{ObservedGeneration: &gen}
	if fedHpa.Status.LastScaleTime != nil {
		// TODO: do we really need to copy value
		t := metav1.NewTime(fedHpa.Status.LastScaleTime.Time)
		updatedStatus.LastScaleTime = &t
	}
	var cpp int32 = 0
	updatedStatus.CurrentCPUUtilizationPercentage = &cpp

	count := int32(0)
	for _, lObj := range current {
		if lObj == nil {
			continue
		}
		lHpa := lObj.(*autoscalingv1.HorizontalPodAutoscaler)
		if lHpa.Status.CurrentCPUUtilizationPercentage != nil {
			*updatedStatus.CurrentCPUUtilizationPercentage += *lHpa.Status.CurrentCPUUtilizationPercentage
			count++
		}
		if updatedStatus.LastScaleTime != nil && lHpa.Status.LastScaleTime != nil &&
			updatedStatus.LastScaleTime.After(lHpa.Status.LastScaleTime.Time) {
			t := metav1.NewTime(lHpa.Status.LastScaleTime.Time)
			updatedStatus.LastScaleTime = &t
		}
		updatedStatus.CurrentReplicas += lHpa.Status.CurrentReplicas
		updatedStatus.DesiredReplicas += lHpa.Status.DesiredReplicas
	}

	// Average out the available current utilisation
	if *updatedStatus.CurrentCPUUtilizationPercentage != 0 {
		*updatedStatus.CurrentCPUUtilizationPercentage /= count
	}

	fmt.Println("\nHPA UPdated Status: -\n", updatedStatus)
	fmt.Println("\nFEDHPA Status: -\n", fedHpa.Status)

	if ((fedHpa.Status.CurrentCPUUtilizationPercentage == nil &&
		*updatedStatus.CurrentCPUUtilizationPercentage != 0) ||
		(fedHpa.Status.CurrentCPUUtilizationPercentage != nil &&
			*updatedStatus.CurrentCPUUtilizationPercentage !=
				*fedHpa.Status.CurrentCPUUtilizationPercentage)) ||
		((fedHpa.Status.LastScaleTime == nil && updatedStatus.LastScaleTime != nil) ||
			(fedHpa.Status.LastScaleTime != nil && updatedStatus.LastScaleTime == nil) ||
			((fedHpa.Status.LastScaleTime != nil && updatedStatus.LastScaleTime != nil) &&
				updatedStatus.LastScaleTime.After(fedHpa.Status.LastScaleTime.Time))) ||
		fedHpa.Status.DesiredReplicas != updatedStatus.DesiredReplicas ||
		fedHpa.Status.CurrentReplicas != updatedStatus.CurrentReplicas {
		// fedHpa is a copied object in reconcile, so ideally it should
		// be ok to use the same object
		fedHpa.Status = updatedStatus
		return fedHpa, true
	}
	return nil, false
}
