package astscan

import "github.com/ngnl47/gobricks/meta"

type Options struct {
	ImportPath string
}

type PackageSet struct {
	Types map[string]meta.TypeSpec
}

func (set *PackageSet) Lookup(ref meta.TypeRef) (meta.TypeSpec, bool) {
	if set == nil {
		return meta.TypeSpec{}, false
	}
	spec, ok := set.Types[ref.FullName]
	if !ok {
		return meta.TypeSpec{}, false
	}
	return spec, true
}

func (set *PackageSet) TypeSpecs() []meta.TypeSpec {
	if set == nil {
		return nil
	}
	specs := make([]meta.TypeSpec, 0, len(set.Types))
	for _, spec := range set.Types {
		specs = append(specs, spec)
	}
	return specs
}
