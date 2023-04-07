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
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func StartAffinityInformer(ctx context.Context, db *memory.Database) {
	informerConstructor := func(factory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return factory.Core().V1().Pods().Informer()
	}

	startResourceInformer(ctx, db, informerConstructor, initAffinityTable, onAddAffinity, onUpdateAffinity, onDelAffinity)
}

type AffinityTable struct {
	db     *memory.Database
	table  *memory.Table
	logger *logrus.Entry
}

func initAffinityTable(db *memory.Database) {
	if _, ok := tables[AffinityTableName]; !ok {
		tables[AffinityTableName] = &AffinityTable{
			db:     db,
			table:  createAffinityTable(db),
			logger: tableLogger(AffinityTableName),
		}
	}
}

func createAffinityTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(AffinityTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: AffinityTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: AffinityTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: AffinityTableName},
		{Name: "weight", Type: sql.Int32, Nullable: false, Source: AffinityTableName},
		{Name: "affinity", Type: sql.Text, Nullable: false, Source: AffinityTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: AffinityTableName},
	}), db.GetForeignKeyCollection())

	db.AddTable(AffinityTableName, table)
	log.Infof("table [%s] created", AffinityTableName)
	return table
}

func (t *AffinityTable) Log() *logrus.Entry {
	return t.logger
}

func (t *AffinityTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, AffinityTableName)
}

func (t *AffinityTable) Insert(ctx *sql.Context, resource interface{}) error {
	pod, ok := resource.(*v1.Pod)
	if !ok {
		return fmt.Errorf("resource is not of type *v1.Pod")
	}
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
			log.Error(err)
			closureDiscard(ctx, err)
		}

		for _, affinityPod := range selectedPods {
			if err := closureAction(ctx, affinityRow(pod, &affinityPod, &preferedTerm)); err != nil {
				log.Error(err)
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

func (t *AffinityTable) Delete(ctx *sql.Context, resource interface{}) error {
	pod, ok := resource.(*v1.Pod)
	if !ok {
		return fmt.Errorf("resource is not of type *v1.Pod")
	}
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseAffinities(ctx, pod, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *AffinityTable) Update(ctx *sql.Context, oldres, newres interface{}) error {
	oldPod, ok := oldres.(*v1.Pod)
	if !ok {
		return fmt.Errorf("oldres is not of type *v1.Pod")
	}
	newPod, ok := newres.(*v1.Pod)
	if !ok {
		return fmt.Errorf("newres is not of type *v1.Pod")
	}

	if err := t.Delete(ctx, oldPod); err != nil {
		return err
	}
	if err := t.Insert(ctx, newPod); err != nil {
		return err
	}
	return nil
}

func onAddAffinity(o interface{}) {
	t := table(AffinityTableName)
	pod := o.(*v1.Pod)
	t.Log().Debugf("adding affinity: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Insert(ctx, pod); err != nil {
		t.Log().Error(err)
	}
}

func onDelAffinity(o interface{}) {
	t := table(AffinityTableName)
	pod := o.(*v1.Pod)
	t.Log().Debugf("deleting affinity: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Delete(ctx, pod); err != nil {
		t.Log().Error(err)
	}
}

func onUpdateAffinity(oldObj interface{}, newObj interface{}) {
	t := table(AffinityTableName)
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	t.Log().Debugf("updating affinity: %s\n", oldPod.Name)
	ctx := sql.NewEmptyContext()
	if err := t.Update(ctx, oldPod, newPod); err != nil {
		t.Log().Error(err)
	}
}
