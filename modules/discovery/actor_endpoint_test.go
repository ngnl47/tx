package discovery

import "testing"

func TestActorEndpointIndexFollowsSnapshot(t *testing.T) {
	d := NewServiceDirectory()
	lookup, ok := any(d).(interface{ LookupActorInstance(string) (string, bool) })
	if !ok {
		t.Fatal("directory lacks actor endpoint lookup")
	}
	a := Instance{ID: "a", Services: []string{"game"}, Endpoint: "a:1", ActorEndpoint: "host:2", Status: StatusRunning}
	b := a
	b.ID = "b"
	for _, step := range []struct {
		instances []Instance
		want      string
	}{
		{[]Instance{a}, "a"}, {[]Instance{a, b}, ""}, {[]Instance{b}, "b"},
	} {
		if err := d.Replace(step.instances); err != nil {
			t.Fatal(err)
		}
		id, found := lookup.LookupActorInstance("host:2")
		if id != step.want || found != (step.want != "") {
			t.Fatalf("lookup = %q, %v; want %q", id, found, step.want)
		}
	}
	d.Invalidate()
	if _, found := lookup.LookupActorInstance("host:2"); found {
		t.Fatal("invalidated address retained")
	}
}
