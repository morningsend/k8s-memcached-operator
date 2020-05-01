package memcached

import (
	"context"
	"reflect"

	morningsendv1alpha1 "github.com/morningsend/memcached-operator/pkg/apis/morningsend/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_memcached")
var defaultMemcachedImage = "memcached:1.6.5-alpine"

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new Memcached Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMemcached{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("memcached-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource Memcached
	err = c.Watch(&source.Kind{Type: &morningsendv1alpha1.Memcached{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner Memcached
	err = c.Watch(&source.Kind{Type: &appsv1.Deployment{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &morningsendv1alpha1.Memcached{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileMemcached implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileMemcached{}

// ReconcileMemcached reconciles a Memcached object
type ReconcileMemcached struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a Memcached object and makes changes based on the state read
// and what is in the Memcached.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileMemcached) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling Memcached")

	// Fetch the Memcached instance
	instance := &morningsendv1alpha1.Memcached{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	deployment := &appsv1.Deployment{}
	err = r.client.Get(
		context.TODO(),
		types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace},
		deployment,
	)

	if err != nil {
		// memcached is CRD is new, we deploy memcached new pods
		if errors.IsNotFound(err) {
			newDep := r.newDeploymentForMemCached(instance)
			reqLogger.Info("creating deployment for memcached",
				"Deployment.Namespace", newDep.Namespace, "Deployment.Name", newDep.Name)

			if err := r.client.Create(context.TODO(), newDep); err != nil {
				reqLogger.Error(
					err, "failed to create memcached deployment",
					"Deployment.Namespace", newDep.Namespace,
					"Deployment.Name", newDep.Name,
				)
				return reconcile.Result{}, err
			} else {
				reqLogger.Info("successfully create deployment")
				return reconcile.Result{}, nil
			}
		}
	}

	currentSize := *deployment.Spec.Replicas
	desiredSize := instance.Spec.ClusterSize

	if desiredSize != currentSize {
		deployment.Spec.Replicas = &desiredSize
		reqLogger.Info("updating memcached",
			"Memcached.Namespace", instance.Namespace, "Memcached.Name", instance.Name, "Memcached.ClusterSize", desiredSize)
		if err := r.client.Update(context.TODO(), deployment); err != nil {
			reqLogger.Error(err, "error updating memcached dpeloyment",
				"Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}

	if instance.Spec.Image != deployment.Spec.Template.Spec.Containers[0].Image {
		deployment.Spec.Template.Spec.Containers[0].Image = instance.Spec.Image
		reqLogger.Info("updating memcached",
			"Memcached.Namespace", instance.Namespace, "Memcached.Name", instance.Name)
		if err := r.client.Update(context.TODO(), deployment); err != nil {
			reqLogger.Error(err, "error updating memcached dpeloyment",
				"Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}

	// Update Memcached.Status if needed
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(instance.Namespace),
		client.MatchingLabels(labelsForMemcached(instance)),
	}
	if err := r.client.List(context.TODO(), podList, listOpts...); err != nil {
		return reconcile.Result{}, nil
	}

	names := getPodNames(podList)
	if !reflect.DeepEqual(names, instance.Status.Nodes) {
		instance.Status.Nodes = names
		if err := r.client.Status().Update(context.TODO(), instance); err != nil {
			reqLogger.Error(err, "failed to update memcached status",
				"Memcached.Namespace", instance.Namespace,
				"Memcached.Name", instance.Name,
			)
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

func labelsForMemcached(m *morningsendv1alpha1.Memcached) map[string]string {
	return map[string]string{"app": "memcached", "memcached_cr": m.Name}
}

func (r *ReconcileMemcached) newDeploymentForMemCached(m *morningsendv1alpha1.Memcached) *appsv1.Deployment {
	labels := labelsForMemcached(m)
	replicas := m.Spec.ClusterSize

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:    m.Name,
						Image:   m.Spec.Image,
						Command: []string{"memcached", "-m", "64", "-o", "modern", "-v"},
						Ports: []corev1.ContainerPort{{
							ContainerPort: 11211,
							Name:          "memcached",
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								"memory": resource.MustParse("64Mi"),
							},
							Limits: corev1.ResourceList{
								"memory": resource.MustParse("80Mi"),
							},
						},
					}},
				},
			},
		},
	}
	controllerutil.SetOwnerReference(m, dep, r.scheme)

	return dep
}

func getPodNames(pods *corev1.PodList) []string {
	len := len(pods.Items)
	names := make([]string, len)
	for i, p := range pods.Items {
		names[i] = p.Name
	}
	return names
}
