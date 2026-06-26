/*
 * Copyright 2020-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

package com.couchbase.client.dcp.message;

import static org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_OPUS_4_6;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.GENERATED;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.REFACTORED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.GITHUB_COPILOT;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.util.CollectionsUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A collections manifest backed directly by the Jackson JSON object model.
 * Scopes and collections are maintained in sorted order by uid (unsigned) to
 * enable O(log N) binary search lookups. Mutations return new instances with
 * shared structural references where possible.
 * <p>
 * Immutable.
 */
@AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = REFACTORED, notes = "Refactored from fastutil map-based implementation to Jackson object model delegation")
public class CollectionsManifest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Comparator<ScopeInfo> SCOPE_UID_ORDER = (a, b) -> Integer.compareUnsigned(a.uid, b.uid);
    private static final Comparator<CollectionInfo> COLLECTION_UID_ORDER =
            (a, b) -> Integer.compareUnsigned(a.uid, b.uid);

    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final CollectionInfo[] EMPTY_COLLECTION_ARRAY = new CollectionInfo[0];

    public static final CollectionsManifest DEFAULT = defaultManifest();
    public static final CollectionsManifest EMPTY_MANIFEST = emptyManifest();

    // -- Hex serializers/deserializers for Jackson --

    @AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
    static class HexLongSerializer extends JsonSerializer<Long> {
        @Override
        public void serialize(Long value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(Long.toUnsignedString(value, 16));
        }
    }

    @AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
    static class HexLongDeserializer extends JsonDeserializer<Long> {
        @Override
        public Long deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return Long.parseUnsignedLong(p.getText(), 16);
        }
    }

    @AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
    static class HexIntSerializer extends JsonSerializer<Integer> {
        @Override
        public void serialize(Integer value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(Integer.toUnsignedString(value, 16));
        }
    }

    @AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
    static class HexIntDeserializer extends JsonDeserializer<Integer> {
        @Override
        public Integer deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return Integer.parseUnsignedInt(p.getText(), 16);
        }
    }

    // -- Binary search helpers (unsigned uid comparison) --

    interface UidAccessor {
        int uid();
    }

    /**
     * Binary search for a uid in a sorted list using unsigned comparison.
     * @return index if found, or -(insertion point) - 1 if not found
     */
    @AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
    private static <T extends UidAccessor> int search(List<T> list, int uid) {
        int lo = 0, hi = list.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = Integer.compareUnsigned(list.get(mid).uid(), uid);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
                return mid;
        }
        return -(lo + 1);
    }

    @AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
    private int searchCollection(int uid) {
        int lo = 0, hi = collectionUids.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = Integer.compareUnsigned(collectionUids[mid], uid);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
                return mid;
        }
        return -(lo + 1);
    }

    // -- Public API types (also serve as the Jackson object model) --

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ScopeInfo implements UidAccessor {
        public static final String DEFAULT_NAME = "_default";

        @JsonSerialize(using = HexIntSerializer.class)
        @JsonDeserialize(using = HexIntDeserializer.class)
        public int uid;
        public String name;
        public List<CollectionInfo> collections;

        ScopeInfo() {
        }

        @Override
        @JsonIgnore
        public int uid() {
            return uid;
        }

        @JsonIgnore
        public int id() {
            return uid;
        }

        @JsonIgnore
        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            return uid == ((ScopeInfo) o).uid;
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid);
        }

        @Override
        public String toString() {
            return "0x" + Integer.toUnsignedString(uid, 16) + ":" + name;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CollectionInfo implements UidAccessor {
        public static final String DEFAULT_NAME = "_default";
        public static final long MAX_TTL_UNDEFINED = -1L;

        @JsonSerialize(using = HexIntSerializer.class)
        @JsonDeserialize(using = HexIntDeserializer.class)
        public int uid;
        public String name;
        public long max_ttl;

        @JsonIgnore
        ScopeInfo parentScope;

        CollectionInfo() {
        }

        @JsonIgnore
        public ScopeInfo scope() {
            return parentScope;
        }

        @Override
        @JsonIgnore
        public int uid() {
            return uid;
        }

        @JsonIgnore
        public int id() {
            return uid;
        }

        @JsonIgnore
        public String name() {
            return name;
        }

        @JsonIgnore
        public long maxTtl() {
            return max_ttl;
        }

        @Override
        public String toString() {
            return "CollectionInfo{" + "id=" + CollectionsUtil.displayCid(uid) + ", name='" + name + '\'' + ", scope="
                    + parentScope + ", maxTtl=" + (max_ttl == MAX_TTL_UNDEFINED ? "<UNDEFINED>" : max_ttl) + '}';
        }
    }

    // -- Internal model and index --

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ManifestJson {
        @JsonSerialize(using = HexLongSerializer.class)
        @JsonDeserialize(using = HexLongDeserializer.class)
        public long uid;
        public List<ScopeInfo> scopes;
    }

    private final ManifestJson model;
    private final int[] collectionUids;
    private final CollectionInfo[] sortedCollections;

    private CollectionsManifest(ManifestJson model) {
        this.model = model;
        int total = 0;
        for (ScopeInfo s : model.scopes) {
            total += s.collections.size();
        }
        if (total == 0) {
            this.collectionUids = EMPTY_INT_ARRAY;
            this.sortedCollections = EMPTY_COLLECTION_ARRAY;
            return;
        }
        int[] uids = new int[total];
        CollectionInfo[] cols = new CollectionInfo[total];
        int idx = 0;
        for (ScopeInfo s : model.scopes) {
            for (CollectionInfo c : s.collections) {
                uids[idx] = c.uid;
                cols[idx] = c;
                idx++;
            }
        }
        Integer[] order = new Integer[total];
        for (int i = 0; i < total; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> Integer.compareUnsigned(uids[a], uids[b]));
        this.collectionUids = new int[total];
        this.sortedCollections = new CollectionInfo[total];
        for (int i = 0; i < total; i++) {
            int src = order[i];
            this.collectionUids[i] = uids[src];
            this.sortedCollections[i] = cols[src];
        }
    }

    private CollectionsManifest(ManifestJson model, CollectionsManifest source) {
        this.model = model;
        this.collectionUids = source.collectionUids;
        this.sortedCollections = source.sortedCollections;
    }

    /** Link each CollectionInfo back to its parent ScopeInfo. */
    private static void linkScopes(ManifestJson m) {
        for (ScopeInfo s : m.scopes) {
            for (CollectionInfo c : s.collections) {
                c.parentScope = s;
            }
        }
    }

    private static void sortManifest(ManifestJson m) {
        m.scopes.sort(SCOPE_UID_ORDER);
        for (ScopeInfo s : m.scopes) {
            s.collections.sort(COLLECTION_UID_ORDER);
        }
    }

    private static CollectionsManifest emptyManifest() {
        ManifestJson m = new ManifestJson();
        m.uid = 0;
        m.scopes = Collections.emptyList();
        return new CollectionsManifest(m);
    }

    private static CollectionsManifest defaultManifest() {
        ManifestJson m = new ManifestJson();
        m.uid = 0;
        ScopeInfo scope = new ScopeInfo();
        scope.uid = 0;
        scope.name = ScopeInfo.DEFAULT_NAME;
        CollectionInfo col = new CollectionInfo();
        col.uid = 0;
        col.name = CollectionInfo.DEFAULT_NAME;
        col.max_ttl = CollectionInfo.MAX_TTL_UNDEFINED;
        col.parentScope = scope;
        scope.collections = new ArrayList<>(1);
        scope.collections.add(col);
        m.scopes = new ArrayList<>(1);
        m.scopes.add(scope);
        return new CollectionsManifest(m);
    }

    // -- Public mutation methods --

    public CollectionsManifest withManifestId(long newManifestUid) {
        ManifestJson newModel = new ManifestJson();
        newModel.uid = newManifestUid;
        newModel.scopes = model.scopes;
        return new CollectionsManifest(newModel, this);
    }

    public CollectionsManifest withScope(long newManifestUid, int newScopeId, String newScopeName) {
        ManifestJson newModel = new ManifestJson();
        newModel.uid = newManifestUid;
        newModel.scopes = new ArrayList<>(model.scopes.size() + 1);
        newModel.scopes.addAll(model.scopes);
        ScopeInfo newScope = new ScopeInfo();
        newScope.uid = newScopeId;
        newScope.name = newScopeName;
        newScope.collections = new ArrayList<>();
        int pos = search(newModel.scopes, newScopeId);
        if (pos < 0) {
            pos = -(pos + 1);
        }
        newModel.scopes.add(pos, newScope);
        return new CollectionsManifest(newModel);
    }

    public CollectionsManifest withoutScope(long newManifestUid, int doomedScopeId) {
        int scopeIdx = search(model.scopes, doomedScopeId);
        if (scopeIdx < 0) {
            if (getUid() == newManifestUid) {
                return this;
            }
            ManifestJson newModel = new ManifestJson();
            newModel.uid = newManifestUid;
            newModel.scopes = model.scopes;
            return new CollectionsManifest(newModel, this);
        }
        ManifestJson newModel = new ManifestJson();
        newModel.uid = newManifestUid;
        newModel.scopes = new ArrayList<>(model.scopes.size());
        for (int i = 0; i < model.scopes.size(); i++) {
            if (i != scopeIdx) {
                newModel.scopes.add(model.scopes.get(i));
            }
        }
        return new CollectionsManifest(newModel);
    }

    public CollectionsManifest withCollection(long newManifestUid, int scopeId, int collectionId, String collectionName,
            long maxTtl) {
        int scopeIdx = search(model.scopes, scopeId);
        if (scopeIdx < 0) {
            throw new IllegalStateException("Unrecognized scope id: " + scopeId);
        }
        ScopeInfo targetScope = model.scopes.get(scopeIdx);

        ManifestJson newModel = new ManifestJson();
        newModel.uid = newManifestUid;
        newModel.scopes = new ArrayList<>(model.scopes.size());
        for (ScopeInfo s : model.scopes) {
            if (s == targetScope) {
                ScopeInfo newScope = new ScopeInfo();
                newScope.uid = s.uid;
                newScope.name = s.name;
                newScope.collections = new ArrayList<>(s.collections.size() + 1);
                newScope.collections.addAll(s.collections);
                CollectionInfo newCol = new CollectionInfo();
                newCol.uid = collectionId;
                newCol.name = collectionName;
                newCol.max_ttl = maxTtl;
                newCol.parentScope = newScope;
                int colPos = search(newScope.collections, collectionId);
                if (colPos < 0) {
                    colPos = -(colPos + 1);
                }
                newScope.collections.add(colPos, newCol);
                newModel.scopes.add(newScope);
            } else {
                newModel.scopes.add(s);
            }
        }
        return new CollectionsManifest(newModel);
    }

    public CollectionsManifest withoutCollection(long newManifestUid, int id) {
        // O(log N) lookup in the flat collection index to confirm existence
        int colIdx = searchCollection(id);
        if (colIdx < 0) {
            LOGGER.debug("can't remove collection id: " + id + " as it was not found");
            if (getUid() == newManifestUid) {
                return this;
            }
            ManifestJson newModel = new ManifestJson();
            newModel.uid = newManifestUid;
            newModel.scopes = model.scopes;
            return new CollectionsManifest(newModel, this);
        }
        // Find the owning scope in this manifest's model
        ScopeInfo owningScope = null;
        for (ScopeInfo s : model.scopes) {
            if (search(s.collections, id) >= 0) {
                owningScope = s;
                break;
            }
        }

        ManifestJson newModel = new ManifestJson();
        newModel.uid = newManifestUid;
        newModel.scopes = new ArrayList<>(model.scopes.size());
        for (ScopeInfo s : model.scopes) {
            if (s == owningScope) {
                ScopeInfo newScope = new ScopeInfo();
                newScope.uid = s.uid;
                newScope.name = s.name;
                newScope.collections = new ArrayList<>(s.collections.size());
                for (CollectionInfo c : s.collections) {
                    if (c.uid != id) {
                        newScope.collections.add(c);
                    }
                }
                newModel.scopes.add(newScope);
            } else {
                newModel.scopes.add(s);
            }
        }
        return new CollectionsManifest(newModel);
    }

    // -- Public query methods --

    public CollectionInfo getCollection(int id) {
        int idx = searchCollection(id);
        return idx >= 0 ? sortedCollections[idx] : null;
    }

    public CollectionInfo getCollection(ScopeInfo scope, String collectionName) {
        // Look up the scope in this manifest by uid to ensure we use current data
        int scopeIdx = search(model.scopes, scope.uid);
        if (scopeIdx < 0) {
            return null;
        }
        ScopeInfo manifestScope = model.scopes.get(scopeIdx);
        for (CollectionInfo c : manifestScope.collections) {
            if (c.name.equals(collectionName)) {
                return c;
            }
        }
        return null;
    }

    public ScopeInfo getScope(String name) {
        for (ScopeInfo s : model.scopes) {
            if (s.name.equals(name)) {
                return s;
            }
        }
        return null;
    }

    public CollectionInfo getCollection(String scopeName, String collectionName) {
        for (ScopeInfo s : model.scopes) {
            if (s.name.equals(scopeName)) {
                for (CollectionInfo c : s.collections) {
                    if (c.name.equals(collectionName)) {
                        return c;
                    }
                }
                return null;
            }
        }
        return null;
    }

    public Stream<ScopeInfo> stream() {
        return model.scopes.stream();
    }

    public long getUid() {
        return model.uid;
    }

    @Override
    public String toString() {
        return "CollectionsManifest{" + "uid=0x" + Long.toUnsignedString(model.uid, 16) + '}';
    }

    public String toDetailedString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CollectionsManifest{uid=0x").append(Long.toUnsignedString(model.uid, 16));
        sb.append(", scopes=").append(model.scopes);
        sb.append(", collections=[");
        boolean first = true;
        for (ScopeInfo s : model.scopes) {
            for (CollectionInfo c : s.collections) {
                if (!first)
                    sb.append(", ");
                sb.append(c);
                first = false;
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    public static CollectionsManifest fromJson(byte[] jsonBytes) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("fromJson: {}", new String(jsonBytes, StandardCharsets.UTF_8));
        }
        ManifestJson manifest = OBJECT_MAPPER.readValue(jsonBytes, ManifestJson.class);
        sortManifest(manifest);
        linkScopes(manifest);
        return new CollectionsManifest(manifest);
    }

    public byte[] toJson() throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(model);
    }

}
