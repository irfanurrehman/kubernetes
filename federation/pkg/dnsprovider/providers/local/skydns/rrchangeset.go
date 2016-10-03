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
	"encoding/json"
	"fmt"
	etcd "github.com/coreos/etcd/client"
	skymsg "github.com/skynetservices/skydns/msg"
	"golang.org/x/net/context"
	"hash/fnv"
	"k8s.io/kubernetes/federation/pkg/dnsprovider"
)

// Compile time check for interface adherence
var _ dnsprovider.ResourceRecordChangeset = &ResourceRecordChangeset{}

type ResourceRecordChangeset struct {
	zone   *Zone
	rrsets *ResourceRecordSets

	additions []dnsprovider.ResourceRecordSet
	removals  []dnsprovider.ResourceRecordSet
}

func (c *ResourceRecordChangeset) Add(rrset dnsprovider.ResourceRecordSet) dnsprovider.ResourceRecordChangeset {
	c.additions = append(c.additions, rrset)
	return c
}

func (c *ResourceRecordChangeset) Remove(rrset dnsprovider.ResourceRecordSet) dnsprovider.ResourceRecordChangeset {
	c.removals = append(c.removals, rrset)
	return c
}

func (c *ResourceRecordChangeset) Apply() error {
	ctx := context.Background()
	skymsg.PathPrefix = c.zone.zones.interface_.etcdPathPrefix

	for _, removal := range c.removals {
		deleteOpts := &etcd.DeleteOptions{
			Recursive: true,
		}
		_, err := c.zone.zones.interface_.etcdKeysAPI.Delete(ctx, skymsg.Path(removal.Name()), deleteOpts)
		if err != nil {
			return err
		}
	}

	for _, addition := range c.additions {
		getOpts := &etcd.GetOptions{}
		setOpts := &etcd.SetOptions{}

		for _, rrdata := range addition.Rrdatas() {
			b, err := json.Marshal(&skymsg.Service{Host: rrdata, Ttl: uint32(addition.Ttl())})
			if err != nil {
				return err
			}
			recordValue := string(b)
			recordLabel := getHash(recordValue)
			recordKey := buildDNSNameString(addition.Name(), recordLabel)

			response, err := c.zone.zones.interface_.etcdKeysAPI.Get(context.Background(), skymsg.Path(recordKey), getOpts)
			if err == nil && response != nil {
				return fmt.Errorf("Key already exist, key: %v", recordKey)
			}

			_, err = c.zone.zones.interface_.etcdKeysAPI.Set(ctx, skymsg.Path(recordKey), recordValue, setOpts)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getHash(text string) string {
	h := fnv.New32a()
	h.Write([]byte(text))
	return fmt.Sprintf("%x", h.Sum32())
}

func buildDNSNameString(labels ...string) string {
	var res string
	for _, label := range labels {
		if res == "" {
			res = label
		} else {
			res = fmt.Sprintf("%s.%s", label, res)
		}
	}
	return res
}
