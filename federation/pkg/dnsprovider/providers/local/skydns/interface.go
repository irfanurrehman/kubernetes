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

package skydns

import (
	"k8s.io/kubernetes/federation/pkg/dnsprovider"
	"k8s.io/kubernetes/federation/pkg/dnsprovider/providers/local/skydns/stubs"
)

// Compile time check for interface adherence
var _ dnsprovider.Interface = Interface{}

type Interface struct {
	etcdKeysAPI    stubs.EtcdKeysAPI
	domain         string
	etcdPathPrefix string
}

// newInterfaceWithStub facilitates stubbing out the underlying etcd
// library for testing purposes.  It returns an provider-independent interface.
func newInterfaceWithStub(etcdKeysAPI stubs.EtcdKeysAPI) *Interface {
	return &Interface{etcdKeysAPI: etcdKeysAPI}
}

func (i Interface) Zones() (zones dnsprovider.Zones, supported bool) {
	return Zones{&i}, true
}
