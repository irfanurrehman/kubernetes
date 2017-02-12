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

package kubefed

import (
	"fmt"
	"io"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	federationapi "k8s.io/kubernetes/federation/apis/federation"
	kubefedinit "k8s.io/kubernetes/federation/pkg/kubefed/init"
	"k8s.io/kubernetes/federation/pkg/kubefed/util"
	"k8s.io/kubernetes/pkg/kubectl/cmd/templates"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/resource"
	kubeinternalclientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/api"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/spf13/cobra"
)

var (
	unjoin_long = templates.LongDesc(`
		Unjoin removes a cluster from a federation.

        Current context is assumed to be a federation endpoint.
        Please use the --context flag otherwise.`)
	unjoin_example = templates.Examples(`
		# Unjoin removes the specified cluster from a federation.
		# Federation control plane's host cluster context name
		# must be specified via the --host-cluster-context flag
		# to properly cleanup the credentials.
		kubectl unjoin foo --host-cluster-context=bar`)
)

// NewCmdUnjoin defines the `unjoin` command that removes a cluster
// from a federation.
func NewCmdUnjoin(f cmdutil.Factory, cmdOut, cmdErr io.Writer, config util.AdminConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "unjoin CLUSTER_NAME --host-cluster-context=HOST_CONTEXT",
		Short:   "Unjoins a cluster from a federation",
		Long:    unjoin_long,
		Example: unjoin_example,
		Run: func(cmd *cobra.Command, args []string) {
			err := unjoinFederation(f, cmdOut, cmdErr, config, cmd, args)
			cmdutil.CheckErr(err)
		},
	}

	util.AddSubcommandFlags(cmd)
	return cmd
}

// unjoinFederation is the implementation of the `unjoin` command.
func unjoinFederation(f cmdutil.Factory, cmdOut, cmdErr io.Writer, config util.AdminConfig, cmd *cobra.Command, args []string) error {
	unjoinFlags, err := util.GetSubcommandFlags(cmd, args)
	if err != nil {
		return err
	}

	cluster, err := popCluster(f, unjoinFlags.Name)
	if err != nil {
		return err
	}
	if cluster == nil {
		fmt.Fprintf(cmdErr, "WARNING: cluster %q not found in federation, so its credentials' secret couldn't be deleted", unjoinFlags.Name)
		return nil
	}

	hostFactory := config.HostFactory(unjoinFlags.Host, unjoinFlags.Kubeconfig)
	clientset, err := hostFactory.ClientSet()
	if err != nil {
		return err
	}

	po := config.PathOptions()
	po.LoadingRules.ExplicitPath = unjoinFlags.Kubeconfig
	clientConfig, err := po.GetStartingConfig()

	secretName := cluster.Spec.SecretRef.Name
	secret, err := clientset.Core().Secrets(unjoinFlags.FederationSystemNamespace).Get(secretName, metav1.GetOptions{})
	if util.IsNotFound(err) {
		// If this is the case, we cannot get the cluster clientset to delete the
		// config map from that cluster and obviously cannot delete the not existing secret
		// We just publish the warning as cluster has already been removed from federation
		fmt.Fprintf(cmdErr, "WARNING: secret %q not found in the host cluster, so it couldn't be deleted", secretName)
	} else if err != nil {
		fmt.Fprintf(cmdErr, "WARNING: Error retrieving secret from the base cluster")
	} else {
		// We need to ensure deleting the config map created in the deregistered cluster
		// This configmap was created when the cluster joined this federation to aid
		// the kube-dns of that cluster to aid service discovery
		err := deleteConfigMapFromCluster(secret, cluster, clientConfig)
		if err != nil {
			fmt.Fprintf(cmdErr, "WARNING: Encountered error in deleting kube-dns configmap, %v", err)
			// We anyways continue to try and delete the secret further
		}

		err = deleteSecret(clientset, cluster.Spec.SecretRef.Name, unjoinFlags.FederationSystemNamespace)
		if err != nil {
			return err
		}
	}

	_, err = fmt.Fprintf(cmdOut, "Successfully removed cluster %q from federation\n", unjoinFlags.Name)
	return err
}

// popCluster fetches the cluster object with the given name, deletes
// it and returns the deleted cluster object.
func popCluster(f cmdutil.Factory, name string) (*federationapi.Cluster, error) {
	mapper, typer := f.Object()
	gvks, _, err := typer.ObjectKinds(&federationapi.Cluster{})
	if err != nil {
		return nil, err
	}
	gvk := gvks[0]
	mapping, err := mapper.RESTMapping(schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}, gvk.Version)
	if err != nil {
		return nil, err
	}
	client, err := f.ClientForMapping(mapping)
	if err != nil {
		return nil, err
	}

	rh := resource.NewHelper(client, mapping)
	obj, err := rh.Get("", name, false)

	if util.IsNotFound(err) {
		// Cluster isn't registered, there isn't anything to be done here.
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	cluster, ok := obj.(*federationapi.Cluster)
	if !ok {
		return nil, fmt.Errorf("unexpected object type: expected \"federation/v1beta1.Cluster\", got %T: obj: %#v", obj, obj)
	}

	// Remove the cluster resource in the federation API server by
	// calling rh.Delete()
	return cluster, rh.Delete("", name)
}

func deleteConfigMapFromCluster(secret *api.Secret, cluster *federationapi.Cluster, config *clientcmdapi.Config) error {
	serverAddress, err := util.GetServerAddress(cluster)
	if err != nil {
		return err
	}
	if serverAddress == "" {
		return fmt.Errorf("failed to get server address for the cluster: %s", cluster.Name)
	}

	clientset, err := util.GetClientsetFromSecret(secret, serverAddress)
	if err != nil  || clientset == nil {
		// There is a possibility that the clientset is nil without any error reported
		return err
	}

	configMap, err := clientset.Core().ConfigMaps(metav1.NamespaceSystem).Get(kubefedinit.KubeDnsName, &metav1.GetOptions{})
	if err != nil {
		return err
	}


	delete(configMap.Data, d)

	return clientset.Core().ConfigMaps(metav1.NamespaceSystem).Delete(kubefedinit.KubeDnsName, &metav1.DeleteOptions{})
}

// deleteSecret deletes the secret with the given name from the host
// cluster.
func deleteSecret(clientset *kubeinternalclientset.Clientset,  name, namespace string) error {
	return clientset.Core().Secrets(namespace).Delete(name, &metav1.DeleteOptions{})
}
