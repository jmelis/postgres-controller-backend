package greeting

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	runtimescheme "sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion = schema.GroupVersion{Group: "greeting.example.com", Version: "v1alpha1"}

	SchemeBuilder = &runtimescheme.Builder{GroupVersion: GroupVersion}

	Scheme = runtime.NewScheme()
)

func init() {
	SchemeBuilder.Register(&Greeting{}, &GreetingList{})
	SchemeBuilder.Register(&GreetingCard{}, &GreetingCardList{})
	SchemeBuilder.Register(&GreetingPolicy{}, &GreetingPolicyList{})
	utilruntime.Must(SchemeBuilder.AddToScheme(Scheme))
}

// --- Greeting ---

type Greeting struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GreetingSpec   `json:"spec,omitempty"`
	Status            GreetingStatus `json:"status,omitempty"`
}

type GreetingSpec struct {
	Name string `json:"name"`
}

type GreetingStatus struct {
	Message string `json:"message,omitempty"`
	Phase   string `json:"phase,omitempty"`
	CardRef string `json:"cardRef,omitempty"`
}

type GreetingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Greeting `json:"items"`
}

func (in *Greeting) DeepCopyObject() runtime.Object {
	out := new(Greeting)
	in.DeepCopyInto(out)
	return out
}

func (in *Greeting) DeepCopyInto(out *Greeting) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
}

func (in *GreetingList) DeepCopyObject() runtime.Object {
	out := new(GreetingList)
	in.DeepCopyInto(out)
	return out
}

func (in *GreetingList) DeepCopyInto(out *GreetingList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Greeting, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *Greeting) GetObjectKind() schema.ObjectKind     { return &in.TypeMeta }
func (in *GreetingList) GetObjectKind() schema.ObjectKind { return &in.TypeMeta }

// --- GreetingCard ---

type GreetingCard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GreetingCardSpec `json:"spec,omitempty"`
}

type GreetingCardSpec struct {
	GreetingName string `json:"greetingName"`
	Message      string `json:"message"`
}

type GreetingCardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GreetingCard `json:"items"`
}

func (in *GreetingCard) DeepCopyObject() runtime.Object {
	out := new(GreetingCard)
	in.DeepCopyInto(out)
	return out
}

func (in *GreetingCard) DeepCopyInto(out *GreetingCard) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
}

func (in *GreetingCardList) DeepCopyObject() runtime.Object {
	out := new(GreetingCardList)
	in.DeepCopyInto(out)
	return out
}

func (in *GreetingCardList) DeepCopyInto(out *GreetingCardList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]GreetingCard, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *GreetingCard) GetObjectKind() schema.ObjectKind     { return &in.TypeMeta }
func (in *GreetingCardList) GetObjectKind() schema.ObjectKind { return &in.TypeMeta }

// --- GreetingPolicy ---

type GreetingPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GreetingPolicySpec `json:"spec,omitempty"`
}

type GreetingPolicySpec struct {
	Prefix string `json:"prefix"`
}

type GreetingPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GreetingPolicy `json:"items"`
}

func (in *GreetingPolicy) DeepCopyObject() runtime.Object {
	out := new(GreetingPolicy)
	in.DeepCopyInto(out)
	return out
}

func (in *GreetingPolicy) DeepCopyInto(out *GreetingPolicy) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
}

func (in *GreetingPolicyList) DeepCopyObject() runtime.Object {
	out := new(GreetingPolicyList)
	in.DeepCopyInto(out)
	return out
}

func (in *GreetingPolicyList) DeepCopyInto(out *GreetingPolicyList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]GreetingPolicy, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *GreetingPolicy) GetObjectKind() schema.ObjectKind     { return &in.TypeMeta }
func (in *GreetingPolicyList) GetObjectKind() schema.ObjectKind { return &in.TypeMeta }
