package oci

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/kuberik/release-controller/api/v1alpha1"
)

// Policy semantics match Flux image-reflector-controller v1.2.3
// (internal/policy/{semver,alphabetical,numerical,filter,factory}.go and
// internal/controller/imagepolicy_controller.go applyPolicy).
//
// Flux picks Latest(); we need an ordering. Mapping:
//   - alphabetical asc: latest is the greatest string (sort.Reverse first);
//     desc: latest is the smallest. Byte-wise string comparison.
//   - numerical asc: latest is the largest float64; desc: the smallest.
//   - semver{range}: highest version satisfying the range. Versions are parsed
//     with fluxcd/pkg/version.ParseVersion (needs three dot-separated parts,
//     then Masterminds semver.NewVersion). Tags outside the range or not
//     parseable can never be Latest, so they are not releases: Discover drops them.
//
// All three are applied to the extracted value (filterTags.extract expanded
// over the pattern match), as Flux does.

type policyKind int

const (
	policyNone policyKind = iota
	policySemver
	policyAlpha
	policyNumeric
)

type policy struct {
	kind       policyKind
	desc       bool
	constraint *semver.Constraints
}

func newPolicy(p *v1alpha1.OrderPolicy) (*policy, error) {
	if p == nil {
		return &policy{kind: policyNone}, nil
	}
	switch {
	case p.SemVer != nil:
		c, err := semver.NewConstraint(p.SemVer.Range)
		if err != nil {
			return nil, fmt.Errorf("invalid semver range %q: %w", p.SemVer.Range, err)
		}
		return &policy{kind: policySemver, constraint: c}, nil
	case p.Alphabetical != nil:
		d, err := order(p.Alphabetical.Order)
		return &policy{kind: policyAlpha, desc: d}, err
	case p.Numerical != nil:
		d, err := order(p.Numerical.Order)
		return &policy{kind: policyNumeric, desc: d}, err
	}
	return &policy{kind: policyNone}, nil
}

func order(o string) (desc bool, err error) {
	switch strings.ToUpper(o) {
	case "", "ASC":
		return false, nil
	case "DESC":
		return true, nil
	}
	return false, fmt.Errorf("invalid order %q, must be asc or desc", o)
}

// parseVersion is fluxcd/pkg/version.ParseVersion (v0.16.0), copied because
// that module requires go 1.26 and semver 3.5.
func parseVersion(v string) (*semver.Version, error) {
	if len(strings.SplitN(v, ".", 3)) != 3 {
		return nil, semver.ErrInvalidSemVer
	}
	return semver.NewVersion(v)
}

// admits reports whether an extracted value can be ordered at all under the
// policy (and, for semver, lies inside the range).
func (p *policy) admits(extracted string) bool {
	switch p.kind {
	case policySemver:
		v, err := parseVersion(extracted)
		return err == nil && p.constraint.Check(v)
	case policyNumeric:
		_, err := strconv.ParseFloat(extracted, 64)
		return err == nil
	}
	return true
}

// less reports a older than b under the policy, on extracted values.
func (p *policy) less(a, b string) bool {
	switch p.kind {
	case policySemver:
		va, ea := parseVersion(a)
		vb, eb := parseVersion(b)
		if ea != nil || eb != nil {
			return false
		}
		return va.LessThan(vb)
	case policyAlpha:
		if p.desc {
			return a > b
		}
		return a < b
	case policyNumeric:
		fa, ea := strconv.ParseFloat(a, 64)
		fb, eb := strconv.ParseFloat(b, 64)
		if ea != nil || eb != nil {
			return false
		}
		if p.desc {
			return fa > fb
		}
		return fa < fb
	}
	return false
}

// extractor is Flux's RegexFilter: a tag is kept if the pattern matches; the
// ordered value is the expanded extract template (or the tag when empty).
type extractor struct {
	re      *regexp.Regexp
	replace string
}

func newExtractor(f *v1alpha1.TagFilter) (*extractor, error) {
	if f == nil {
		return nil, nil
	}
	re, err := regexp.Compile(f.Pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regular expression pattern '%s': %w", f.Pattern, err)
	}
	return &extractor{re: re, replace: f.Extract}, nil
}

// apply returns the extracted value and whether the tag passes the filter.
func (e *extractor) apply(tag string) (string, bool) {
	if e == nil {
		return tag, true
	}
	sm := e.re.FindStringSubmatchIndex(tag)
	if len(sm) == 0 {
		return "", false
	}
	if e.replace == "" {
		return tag, true
	}
	return string(e.re.ExpandString(nil, e.replace, tag, sm)), true
}
