package main

// GreetingSpec is the user-provided spec for a Greeting resource.
type GreetingSpec struct {
	Name string `json:"name"`
}

// GreetingStatus is the controller-managed status for a Greeting resource.
type GreetingStatus struct {
	Message string `json:"message,omitempty"`
	Phase   string `json:"phase,omitempty"`
	CardRef string `json:"cardRef,omitempty"`
}

// GreetingCardSpec is the controller-managed spec for a GreetingCard resource.
type GreetingCardSpec struct {
	GreetingName string `json:"greetingName"`
	Message      string `json:"message"`
}

// GreetingCardStatus is the status for a GreetingCard resource (currently unused).
type GreetingCardStatus struct{}

// GreetingPolicySpec is the user-provided spec for a GreetingPolicy resource.
type GreetingPolicySpec struct {
	Prefix string `json:"prefix"`
}

// GreetingPolicyStatus is the status for a GreetingPolicy resource (currently unused).
type GreetingPolicyStatus struct{}
