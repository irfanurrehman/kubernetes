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
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	core "k8s.io/client-go/testing"
	federationapi "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	fakefedclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_clientset/fake"
	testutil "k8s.io/kubernetes/federation/pkg/federation-controller/util/test"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
	autoscalingv1 "k8s.io/kubernetes/pkg/apis/autoscaling/v1"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	fakekubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/clientset/fake"
	//	"github.com/golang/glog"
	//	"github.com/stretchr/testify/assert"
)

const (
	horizontalpodautoscalers = "horizontalpodautoscalers"
	k8s1                     = "k8s-1"
	k8s2                     = "k8s-2"
)

func TestAutoscalerController(t *testing.T) {

	//cluster1 := testutil.NewCluster("cluster1", apiv1.ConditionTrue)
	//cluster2 := testutil.NewCluster("cluster2", apiv1.ConditionTrue)

	fedclientset := fakefedclientset.NewSimpleClientset()
	fedhpawatch := watch.NewFake()
	fedclientset.PrependWatchReactor(horizontalpodautoscalers, core.DefaultWatchReactor(fedhpawatch, nil))

	fedclientset.Federation().Clusters().Create(testutil.NewCluster(k8s1, apiv1.ConditionTrue))
	fedclientset.Federation().Clusters().Create(testutil.NewCluster(k8s2, apiv1.ConditionTrue))

	kube1clientset := fakekubeclientset.NewSimpleClientset()
	kube1hpawatch := watch.NewFake()
	kube1clientset.PrependWatchReactor(horizontalpodautoscalers, core.DefaultWatchReactor(kube1hpawatch, nil))
	kube2clientset := fakekubeclientset.NewSimpleClientset()
	kube2hpawatch := watch.NewFake()
	kube2clientset.PrependWatchReactor(horizontalpodautoscalers, core.DefaultWatchReactor(kube2hpawatch, nil))

	fedInformerClientFactory := func(cluster *federationapi.Cluster) (kubeclientset.Interface, error) {
		switch cluster.Name {
		case k8s1:
			return kube1clientset, nil
		case k8s2:
			return kube2clientset, nil
		default:
			return nil, fmt.Errorf("Unknown cluster: %v", cluster.Name)
		}
	}

	hpaController := NewAutoscalerController(fedclientset)
	hpaController.autoscalerReviewDelay = 10 * time.Millisecond //1 * time.Second
	hpaController.clusterAvailableDelay = 20 * time.Millisecond
	hpaController.updateTimeout = 5 * time.Second

	hpaFedinformer := testutil.ToFederatedInformerForTestOnly(hpaController.fedInformer)
	hpaFedinformer.SetClientFactory(fedInformerClientFactory)

	stopChan := make(chan struct{})
	defer close(stopChan)
	hpaController.Run(stopChan)

	hpa := newHpaWithReplicas("myautoscaler", newInt32(1), newInt32(70), 5)
	hpa, _ = fedclientset.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceDefault).Create(hpa)
	fedhpawatch.Add(hpa)
	time.Sleep(1 * time.Second)

	hpa1, _ := kube1clientset.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceDefault).Get(hpa.Name, metav1.GetOptions{})
	kube1hpawatch.Add(hpa1)
	fmt.Println("The HPA in cluster 1 is:", hpa1)
	hpa1.Status.CurrentReplicas = hpa1.Spec.MaxReplicas
	hpa1.Status.DesiredReplicas = hpa1.Spec.MaxReplicas
	var lUtilisation1 int32 = 10
	lUtilisation1 = *hpa1.Spec.TargetCPUUtilizationPercentage - int32(10)
	hpa1.Status.CurrentCPUUtilizationPercentage = &lUtilisation1
	var t1 metav1.Time = metav1.Now()
	hpa1.Status.LastScaleTime = &t1
	kube1clientset.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceDefault).UpdateStatus(hpa1)

	hpa2, _ := kube2clientset.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceDefault).Get(hpa.Name, metav1.GetOptions{})
	fmt.Println("The HPA in cluster 2 is:", hpa2)
	kube2hpawatch.Add(hpa2)
	hpa2.Status.CurrentReplicas = hpa2.Spec.MaxReplicas
	hpa2.Status.DesiredReplicas = hpa2.Spec.MaxReplicas
	var lUtilisation2 int32 = 10
	lUtilisation2 = *hpa2.Spec.TargetCPUUtilizationPercentage - int32(20)
	hpa2.Status.CurrentCPUUtilizationPercentage = &lUtilisation2
	var t2 metav1.Time = metav1.Now()
	hpa2.Status.LastScaleTime = &t2
	kube2clientset.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceDefault).UpdateStatus(hpa2)

	time.Sleep(1 * time.Second)

	updated_hpa, _ := fedclientset.AutoscalingV1().HorizontalPodAutoscalers(metav1.NamespaceDefault).Get(hpa.Name, metav1.GetOptions{})

	fmt.Println("The updated HPA in federation is:", updated_hpa)

}

/*func WaitForHpaStoreUpdate(store util.FederatedReadOnlyStore, clusterName, key string, desiredHpa *autoscalingv1.HorizontalPodAutoscaler, timeout time.Duration) error {
	retryInterval := 200 * time.Millisecond
	err := wait.PollImmediate(retryInterval, timeout, func() (bool, error) {
		obj, found, err := store.GetByKey(clusterName, key)
		if !found || err != nil {
			glog.Infof("%s is not in the store", key)
			return false, err
		}
		equal := hpaIsEqual(*obj.(*autoscalingv1.HorizontalPodAutoscaler), *desiredHpa)
		if !equal {
			glog.Infof("wrong content in the store expected:\n%v\nactual:\n%v\n", *desiredHpa, *obj.(*autoscalingv1.HorizontalPodAutoscaler))
		}
		return equal, err
	})
	return err
}

func GetHpaFromChan(c chan runtime.Object) *autoscalingv1.HorizontalPodAutoscaler {
	if hpa := GetObjectFromChan(c); hpa == nil {
		return nil
	} else {
		return hpa.(*autoscalingv1.HorizontalPodAutoscaler)
	}
} */

func hpaIsEqual(a, b autoscalingv1.HorizontalPodAutoscaler) bool {
	// Clear the SelfLink and ObjectMeta.Finalizers since they will be different
	// in resoure in federation control plane and resource in underlying cluster.
	a.SelfLink = ""
	b.SelfLink = ""
	a.ObjectMeta.Finalizers = []string{}
	b.ObjectMeta.Finalizers = []string{}
	return reflect.DeepEqual(a, b)
}

func newInt32(val int32) *int32 {
	p := new(int32)
	*p = val
	return p
}

func newHpaWithReplicas(name string, min, targetUtilisation *int32, max int32) *autoscalingv1.HorizontalPodAutoscaler {
	return &autoscalingv1.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: apiv1.NamespaceDefault,
			SelfLink:  "/api/mylink",
		},
		Spec: autoscalingv1.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv1.CrossVersionObjectReference{
				Kind: "HorizontalPodAutoscaler",
				Name: "myhpa",
			},
			MinReplicas:                    min,
			MaxReplicas:                    max,
			TargetCPUUtilizationPercentage: targetUtilisation,
		},
	}
}
