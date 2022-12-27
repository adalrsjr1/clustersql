package services

import (
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

var (
	Clientset   kubernetes.Interface
	ClientsetVS *metricsv.Clientset
)

func StartKubernetes() {
	log := logrus.StandardLogger()
	var kubeConfig *rest.Config
	inClusterConfig, err := rest.InClusterConfig()
	kubeConfig = inClusterConfig
	if err != nil {
		log.Warnf("failed to connect whitin the cluster, fallback to use homedir config")
		userHomeDir, err := os.UserHomeDir()
		if err != nil {
			log.Errorf("error getting user home dir: %s : %v\n", userHomeDir, err)
		}

		kubeConfigPath := filepath.Join(userHomeDir, ".kube", "config")
		log.Infof("using kubeconfig '%s'", kubeConfigPath)

		homeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
		if err != nil {
			log.Errorf("error getting Kubernetes config at %s: %v", kubeConfigPath, err)
		}
		kubeConfig = homeConfig
	}

	Clientset, err = kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.Errorf("error getting kubernetes clientset :%v")
	}

	ClientsetVS, err = metricsv.NewForConfig(kubeConfig)
	if err != nil {
		log.Errorf("error getting kubernetes metricsv :%v")
	}

}
