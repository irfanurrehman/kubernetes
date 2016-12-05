/*
Copyright 2017 The Kubernetes Authors.

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

package hpa

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	FederatedHpaTargetRefAnnotation = "federation.kubernetes.io/hpa-targetref"
)

// List of clusters represented by names as appearing in federation cluster resources.
// This is set by federation hpa and used by target objects to restrict the federated
// target objects to only these clusters.
type ClusterNames struct {
	Names []string
}

func (cn *ClusterNames) String() string {
	annotationBytes, _ := json.Marshal(cn)
	return string(annotationBytes[:])
}

func GetHpaTargetrefAnnotation(obj runtime.Object) (*ClusterNames, error) {
	// TODO: do we need an object type check here ?
	accessor, _ := meta.Accessor(obj)
	targetObjAnno := accessor.GetAnnotations()
	if targetObjAnno == nil {
		return nil, nil
	}

	targetObjAnnoString := targetObjAnno[FederatedHpaTargetRefAnnotation]
	clusterNames := &ClusterNames{}
	if err := json.Unmarshal([]byte(targetObjAnnoString), clusterNames); err != nil {
		return nil, err
	}
	return clusterNames, nil
}

func SetHpaTargetrefAnnotation(obj runtime.Object, clusterNames ClusterNames) runtime.Object {
	accessor, _ := meta.Accessor(obj)
	anno := accessor.GetAnnotations()
	if anno == nil {
		anno = make(map[string]string)
		accessor.SetAnnotations(anno)
	}
	anno[FederatedHpaTargetRefAnnotation] = clusterNames.String()
	return obj
}
