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

package planner

// Planner is a simple scheduler used to distribute replicas in federated clusters.
// It decides how many out of the given replicas/instances should be placed in
// each of the federated clusters for the given object.
type Planner interface {
	Plan(replicasToDistribute int64, availableClusters []string, currentReplicaCount map[string]int64,
	estimatedCapacity map[string]int64, objectKey string) (plan map[string]int64, overflow map[string]int64)
}
