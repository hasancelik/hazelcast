package com.hazelcast.internal.partition.membergroup;

import com.hazelcast.cluster.Member;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * NodeAwareMemberGroupFactory is responsible for MemberGroups
 * creation according to the host metadata provided by
 * {@link DiscoveryStrategy#discoverLocalMetadata()}
 * @since 4.2
 */
public class NodeAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Map<String, MemberGroup> groups = new HashMap<String, MemberGroup>();
        for (Member member : allMembers) {
            final String nodeInfo = member.getAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE);
            if (nodeInfo == null) {
                throw new IllegalArgumentException("Not enough metadata information is provided. "
                        + "Kubernetes node name information must be provided with NODE_AWARE partition group.");
            }
            MemberGroup group = groups.get(nodeInfo);
            if (group == null) {
                group = new DefaultMemberGroup();
                groups.put(nodeInfo, group);
            }
            group.addMember(member);
        }
        return new HashSet<MemberGroup>(groups.values());
    }
}
