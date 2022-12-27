package tables

import (
	"context"
	"fmt"

	"github.com/adalrsjr1/sqlcluster/internal/services"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

const affinityTableName = "Affinity"

var (
	affinityTable *AffinityTable
	affinityLog   = logrus.WithFields(
		logrus.Fields{
			"table": affinityTableName,
		},
	)
)

func StartAffinityInformer(ctx context.Context, db *memory.Database) {
	factory := informers.NewSharedInformerFactory(services.Clientset, 0)
	informer := factory.Core().V1().Pods().Informer()

	defer runtime.HandleCrash()

	// start informer ->
	go factory.Start(ctx.Done())

	// start to sync and call list
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	initAffinityTable(db, informer)
	// informer event handler
	affinityTable.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAddAffinity,
		UpdateFunc: onUpdateAffinity,
		DeleteFunc: onDelAffinity,
	})

	<-ctx.Done()
}

type AffinityTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.SharedIndexInformer
}

func initAffinityTable(db *memory.Database, informer cache.SharedIndexInformer) {
	if affinityTable != nil {
		affinityLog.Warn("podTable name is empty")
		return
	}
	affinityTable = &AffinityTable{
		db:       db,
		table:    createAffinityTable(db),
		informer: informer,
	}
}

func createAffinityTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(affinityTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: affinityTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: affinityTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: affinityTableName},
		{Name: "weight", Type: sql.Int32, Nullable: false, Source: affinityTableName},
		{Name: "affinity", Type: sql.Text, Nullable: false, Source: affinityTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: affinityTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(affinityTableName, table)
	affinityLog.Infof("table [%s] created", affinityTableName)
	return table
}

func (t *AffinityTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, affinityTableName)
}

func (t *AffinityTable) Insert(ctx *sql.Context, pod *v1.Pod) error {
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return transverseAffinities(ctx, pod, inserter.StatementBegin, inserter.StatementComplete, inserter.Insert, inserter.DiscardChanges)
}

func transverseAffinities(ctx *sql.Context, pod *v1.Pod,
	closureBegin func(*sql.Context),
	closureComplete func(*sql.Context) error,
	closureAction func(*sql.Context, sql.Row) error,
	closureDiscard func(*sql.Context, error) error) error {

	preferedTerms := getAffinity(pod).PreferredDuringSchedulingIgnoredDuringExecution
	closureBegin(ctx)

	for _, preferedTerm := range preferedTerms {
		labelSelector := preferedTerm.PodAffinityTerm.LabelSelector
		selectedPods, err := lookupPods(ctx, labelSelector)
		if err != nil {
			affinityLog.Error(err)
			closureDiscard(ctx, err)
		}

		for _, affinityPod := range selectedPods {
			if err := closureAction(ctx, affinityRow(pod, &affinityPod, &preferedTerm)); err != nil {
				affinityLog.Error(err)
				closureDiscard(ctx, err)
			}
		}
	}

	return closureComplete(ctx)
}

func getAffinity(pod *v1.Pod) *v1.PodAffinity {
	if pod.Spec.Affinity != nil {
		if pod.Spec.Affinity.PodAffinity != nil {
			if pod.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
				return pod.Spec.Affinity.PodAffinity
			}
		}
	}

	return &v1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution:  []v1.PodAffinityTerm{},
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{},
	}
}

func lookupPods(ctx context.Context, labelSelector *metav1.LabelSelector) ([]v1.Pod, error) {
	apiSelector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return nil, err
	}
	pods, err := services.Clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: apiSelector.String(),
	})
	if err != nil {
		return nil, err
	}

	// use the snippet below to filter out pods in memory instead of get them filtered
	// selected := []v1.Pod{}
	// selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	// if err != nil {
	// 	return nil, err
	// }
	// for _, pod := range pods.Items {
	// if selector.Matches(labels.Set(pod.Labels)) {
	// 	selected = append(selected, pod)
	// }

	// }
	return pods.Items, nil
}

func affinityRow(pod, affinityPod *v1.Pod, preferedTerm *v1.WeightedPodAffinityTerm) sql.Row {
	return sql.NewRow(string(pod.UID), pod.Name, pod.Namespace, preferedTerm.Weight, affinityPod.Name, pod.CreationTimestamp.Time)
}

func (t *AffinityTable) Delete(ctx *sql.Context, pod *v1.Pod) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseAffinities(ctx, pod, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *AffinityTable) Update(ctx *sql.Context, oldPod, newPod *v1.Pod) error {
	if err := t.Delete(ctx, oldPod); err != nil {
		return err
	}
	if err := t.Insert(ctx, newPod); err != nil {
		return err
	}
	return nil
}

func onAddAffinity(o interface{}) {
	pod := o.(*v1.Pod)
	affinityLog.Debugf("adding affinity: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := affinityTable.Insert(ctx, pod); err != nil {
		affinityLog.Error(err)
	}
}

func onDelAffinity(o interface{}) {
	pod := o.(*v1.Pod)
	affinityLog.Debugf("deleting affinity: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := affinityTable.Delete(ctx, pod); err != nil {
		affinityLog.Error(err)
	}
}

func onUpdateAffinity(oldObj interface{}, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	affinityLog.Debugf("updating affinity: %s\n", oldPod.Name)
	ctx := sql.NewEmptyContext()
	if err := affinityTable.Update(ctx, oldPod, newPod); err != nil {
		affinityLog.Error(err)
	}
}
