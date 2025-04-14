//go:build !ignore_autogenerated

/*
Copyright 2025.

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

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseDeployment) DeepCopyInto(out *ReleaseDeployment) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseDeployment.
func (in *ReleaseDeployment) DeepCopy() *ReleaseDeployment {
	if in == nil {
		return nil
	}
	out := new(ReleaseDeployment)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ReleaseDeployment) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseDeploymentList) DeepCopyInto(out *ReleaseDeploymentList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ReleaseDeployment, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseDeploymentList.
func (in *ReleaseDeploymentList) DeepCopy() *ReleaseDeploymentList {
	if in == nil {
		return nil
	}
	out := new(ReleaseDeploymentList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ReleaseDeploymentList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseDeploymentSpec) DeepCopyInto(out *ReleaseDeploymentSpec) {
	*out = *in
	in.ReleasesRepository.DeepCopyInto(&out.ReleasesRepository)
	in.TargetRepository.DeepCopyInto(&out.TargetRepository)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseDeploymentSpec.
func (in *ReleaseDeploymentSpec) DeepCopy() *ReleaseDeploymentSpec {
	if in == nil {
		return nil
	}
	out := new(ReleaseDeploymentSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseDeploymentStatus) DeepCopyInto(out *ReleaseDeploymentStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseDeploymentStatus.
func (in *ReleaseDeploymentStatus) DeepCopy() *ReleaseDeploymentStatus {
	if in == nil {
		return nil
	}
	out := new(ReleaseDeploymentStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseNomination) DeepCopyInto(out *ReleaseNomination) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseNomination.
func (in *ReleaseNomination) DeepCopy() *ReleaseNomination {
	if in == nil {
		return nil
	}
	out := new(ReleaseNomination)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ReleaseNomination) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseNominationList) DeepCopyInto(out *ReleaseNominationList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ReleaseNomination, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseNominationList.
func (in *ReleaseNominationList) DeepCopy() *ReleaseNominationList {
	if in == nil {
		return nil
	}
	out := new(ReleaseNominationList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ReleaseNominationList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseNominationSpec) DeepCopyInto(out *ReleaseNominationSpec) {
	*out = *in
	if in.ReleaseDeploymentRef != nil {
		in, out := &in.ReleaseDeploymentRef, &out.ReleaseDeploymentRef
		*out = new(v1.LocalObjectReference)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseNominationSpec.
func (in *ReleaseNominationSpec) DeepCopy() *ReleaseNominationSpec {
	if in == nil {
		return nil
	}
	out := new(ReleaseNominationSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseNominationStatus) DeepCopyInto(out *ReleaseNominationStatus) {
	*out = *in
	if in.NominatedRelease != nil {
		in, out := &in.NominatedRelease, &out.NominatedRelease
		*out = new(string)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseNominationStatus.
func (in *ReleaseNominationStatus) DeepCopy() *ReleaseNominationStatus {
	if in == nil {
		return nil
	}
	out := new(ReleaseNominationStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Repository) DeepCopyInto(out *Repository) {
	*out = *in
	if in.Auth != nil {
		in, out := &in.Auth, &out.Auth
		*out = new(v1.LocalObjectReference)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Repository.
func (in *Repository) DeepCopy() *Repository {
	if in == nil {
		return nil
	}
	out := new(Repository)
	in.DeepCopyInto(out)
	return out
}
