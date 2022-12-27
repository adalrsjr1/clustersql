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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

const nodeAffinityTableName = "Node_Affinity"

var (
	nodeAffinityTable *NodeAffinityTable
	nodeAffinityLog   = logrus.WithFields(
		logrus.Fields{
			"table": nodeAffinityTableName,
		},
	)
)

func StartNodeAffinityInformer(ctx context.Context, db *memory.Database) {
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

	initNodeAffinityTable(db, informer)
	// informer event handler
	nodeAffinityTable.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAddNodeAffinity,
		UpdateFunc: onUpdateNodeAffinity,
		DeleteFunc: onDelNodeAffinity,
	})

	<-ctx.Done()
}

type NodeAffinityTable struct {
	db       *memory.Database
	table    *memory.Table
	informer cache.SharedIndexInformer
}

func initNodeAffinityTable(db *memory.Database, informer cache.SharedIndexInformer) {
	if nodeAffinityTable != nil {
		nodeAffinityLog.Warn("podTable name is empty")
		return
	}
	nodeAffinityTable = &NodeAffinityTable{
		db:       db,
		table:    createNodeAffinityTable(db),
		informer: informer,
	}
}

func createNodeAffinityTable(db *memory.Database) *memory.Table {
	table := memory.NewTable(nodeAffinityTableName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "uid", Type: sql.Text, Nullable: false, Source: nodeAffinityTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: nodeAffinityTableName},
		{Name: "namespace", Type: sql.Text, Nullable: false, Source: nodeAffinityTableName},
		{Name: "weight", Type: sql.Int32, Nullable: false, Source: nodeAffinityTableName},
		{Name: "affinity", Type: sql.Text, Nullable: false, Source: nodeAffinityTableName},
		{Name: "created_at", Type: sql.Datetime, Nullable: false, Source: nodeAffinityTableName},
	}), db.GetForeignKeyCollection())
	db.AddTable(nodeAffinityTableName, table)
	nodeAffinityLog.Infof("table [%s] created", nodeAffinityTableName)
	return table
}

func (t *NodeAffinityTable) Drop(ctx *sql.Context) error {
	return t.db.DropTable(ctx, nodeAffinityTableName)
}

func (t *NodeAffinityTable) Insert(ctx *sql.Context, pod *v1.Pod) error {
	inserter := t.table.Inserter(ctx)
	defer inserter.Close(ctx)

	return transverseNodeAffinities(ctx, pod, inserter.StatementBegin, inserter.StatementComplete, inserter.Insert, inserter.DiscardChanges)
}

func transverseNodeAffinities(ctx *sql.Context, pod *v1.Pod,
	closureBegin func(*sql.Context),
	closureComplete func(*sql.Context) error,
	closureAction func(*sql.Context, sql.Row) error,
	closureDiscard func(*sql.Context, error) error) error {

	preferedTerms := getNodeAffinity(pod).PreferredDuringSchedulingIgnoredDuringExecution
	closureBegin(ctx)

	for _, preferedTerm := range preferedTerms {
		for _, nodeSelector := range preferedTerm.Preference.MatchExpressions {
			selectedNodes, err := lookupNodes(ctx, &nodeSelector)
			if err != nil {
				nodeAffinityLog.Error(err)
				closureDiscard(ctx, err)
			}

			for _, affinityNode := range selectedNodes {
				if err := closureAction(ctx, affinityNodeRow(pod, &affinityNode, &preferedTerm)); err != nil {
					nodeAffinityLog.Error(err)
					closureDiscard(ctx, err)
				}
			}
		}
	}

	return closureComplete(ctx)
}

func getNodeAffinity(pod *v1.Pod) *v1.NodeAffinity {
	if pod.Spec.Affinity != nil {
		if pod.Spec.Affinity.NodeAffinity != nil {
			if pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
				return pod.Spec.Affinity.NodeAffinity
			}
		}
	}

	return &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{},
		},
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
	}
}

func lookupNodes(ctx context.Context, labelSelector *v1.NodeSelectorRequirement) ([]v1.Node, error) {

	var op selection.Operator
	switch labelSelector.Operator {
	case v1.NodeSelectorOpIn:
		op = selection.In
	case v1.NodeSelectorOpNotIn:
		op = selection.NotIn
	case v1.NodeSelectorOpExists:
		op = selection.Exists
	case v1.NodeSelectorOpDoesNotExist:
		op = selection.DoesNotExist
	case v1.NodeSelectorOpGt:
		op = selection.GreaterThan
	case v1.NodeSelectorOpLt:
		op = selection.LessThan
	default:
		return nil, fmt.Errorf("cannot convert NodeSelectorRequirment operator into a proper Selector operator")
	}

	r, err := labels.NewRequirement(labelSelector.Key, op, labelSelector.Values)
	if err != nil {
		return nil, err
	}
	selector := labels.NewSelector()
	selector = selector.Add(*r)

	nodes, err := services.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
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
	return nodes.Items, nil
}

func affinityNodeRow(pod *v1.Pod, affinityNode *v1.Node, preferedTerm *v1.PreferredSchedulingTerm) sql.Row {
	return sql.NewRow(string(pod.UID), pod.Name, pod.Namespace, preferedTerm.Weight, affinityNode.Name, pod.CreationTimestamp.Time)
}

func (t *NodeAffinityTable) Delete(ctx *sql.Context, pod *v1.Pod) error {
	deleter := t.table.Deleter(ctx)
	defer deleter.Close(ctx)

	return transverseNodeAffinities(ctx, pod, deleter.StatementBegin, deleter.StatementComplete, deleter.Delete, deleter.DiscardChanges)
}

func (t *NodeAffinityTable) Update(ctx *sql.Context, oldPod, newPod *v1.Pod) error {
	if err := t.Delete(ctx, oldPod); err != nil {
		return err
	}
	if err := t.Insert(ctx, newPod); err != nil {
		return err
	}
	return nil
}

func onAddNodeAffinity(o interface{}) {
	pod := o.(*v1.Pod)
	nodeAffinityLog.Debugf("adding affinity: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := nodeAffinityTable.Insert(ctx, pod); err != nil {
		nodeAffinityLog.Error(err)
	}
}

func onDelNodeAffinity(o interface{}) {
	pod := o.(*v1.Pod)
	nodeAffinityLog.Debugf("deleting affinity: %s\n", pod.Name)
	ctx := sql.NewEmptyContext()
	if err := nodeAffinityTable.Delete(ctx, pod); err != nil {
		nodeAffinityLog.Error(err)
	}
}

func onUpdateNodeAffinity(oldObj interface{}, newObj interface{}) {
	oldPod := oldObj.(*v1.Pod)
	newPod := newObj.(*v1.Pod)
	nodeAffinityLog.Debugf("updating affinity: %s\n", oldPod.Name)
	ctx := sql.NewEmptyContext()
	if err := nodeAffinityTable.Update(ctx, oldPod, newPod); err != nil {
		nodeAffinityLog.Error(err)
	}
}
