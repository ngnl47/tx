package actor

import (
	"strconv"
	"testing"

	"github.com/asynkron/protoactor-go/cluster"
)

const (
	rollingTopologyMemberCount   = 10
	rollingTopologyIdentityCount = 10_000
)

func TestVirtualPlacementRollingReplacementCanMoveIdentityMoreThanOnce(t *testing.T) {
	members := rollingMembers(rollingTopologyMemberCount, 0)
	identities := rollingIdentities(rollingTopologyIdentityCount)
	owners := resolveRollingOwners(members, identities)
	moves := make([]uint16, len(identities))

	for slot := range members {
		without := membersWithoutSlot(members, slot)
		applyRollingTopology(without, identities, owners, moves)

		members[slot] = rollingMember(slot, 1)
		applyRollingTopology(members, identities, owners, moves)
	}

	moved := 0
	repeated := 0
	totalMoves := 0
	for _, count := range moves {
		if count > 0 {
			moved++
		}
		if count > 1 {
			repeated++
		}
		totalMoves += int(count)
	}
	if moved != len(identities) {
		t.Fatalf("moved identities = %d, want %d", moved, len(identities))
	}
	if repeated == 0 {
		t.Fatal("rolling replacement did not move any identity more than once")
	}
	t.Logf("rolling replacement identities=%d totalMoves=%d repeated=%d", len(identities), totalMoves, repeated)
}

func BenchmarkVirtualPlacementRollingReplacement(b *testing.B) {
	members := rollingMembers(rollingTopologyMemberCount, 0)
	identities := rollingIdentities(rollingTopologyIdentityCount)
	owners := resolveRollingOwners(members, identities)
	without := make(cluster.Members, 0, len(members)-1)
	replacements := make(cluster.Members, b.N)
	for index := range replacements {
		replacements[index] = rollingMember(index%len(members), index/len(members)+1)
	}

	totalMoves := 0
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		slot := index % len(members)
		without = appendMembersWithoutSlot(without[:0], members, slot)
		totalMoves += applyRollingTopology(without, identities, owners, nil)

		members[slot] = replacements[index]
		totalMoves += applyRollingTopology(members, identities, owners, nil)
	}
	b.StopTimer()
	b.ReportMetric(2, "topologies/op")
	b.ReportMetric(rollingTopologyIdentityCount, "identities/topology")
	b.ReportMetric(float64(totalMoves)/float64(b.N), "moves/op")
}

func rollingMembers(count, generation int) cluster.Members {
	members := make(cluster.Members, count)
	for slot := range members {
		members[slot] = rollingMember(slot, generation)
	}
	return members
}

func rollingMember(slot, generation int) *cluster.Member {
	return &cluster.Member{
		Id:    "game-" + strconv.Itoa(slot) + "-" + strconv.Itoa(generation),
		Host:  "game-" + strconv.Itoa(slot) + "-" + strconv.Itoa(generation),
		Port:  1,
		Kinds: []string{"role"},
	}
}

func rollingIdentities(count int) []string {
	identities := make([]string, count)
	for index := range identities {
		identities[index] = "role/role-" + strconv.Itoa(index+1)
	}
	return identities
}

func resolveRollingOwners(members cluster.Members, identities []string) []string {
	rendezvous := cluster.NewRendezvous()
	rendezvous.UpdateMembers(members)
	owners := make([]string, len(identities))
	for index, identity := range identities {
		owners[index] = rendezvous.GetByIdentity(identity)
	}
	return owners
}

func applyRollingTopology(members cluster.Members, identities, owners []string, moves []uint16) int {
	rendezvous := cluster.NewRendezvous()
	rendezvous.UpdateMembers(members)
	changed := 0
	for index, identity := range identities {
		owner := rendezvous.GetByIdentity(identity)
		if owner == owners[index] {
			continue
		}
		owners[index] = owner
		changed++
		if moves != nil {
			moves[index]++
		}
	}
	return changed
}

func membersWithoutSlot(members cluster.Members, slot int) cluster.Members {
	return appendMembersWithoutSlot(make(cluster.Members, 0, len(members)-1), members, slot)
}

func appendMembersWithoutSlot(target, members cluster.Members, slot int) cluster.Members {
	target = append(target, members[:slot]...)
	return append(target, members[slot+1:]...)
}
