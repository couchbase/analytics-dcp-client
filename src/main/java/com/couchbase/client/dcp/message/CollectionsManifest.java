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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hyracks.util.fastutil.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.dcp.util.CollectionsUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * A collections manifest optimized for lookups by collection ID,
 * and for evolution via DCP system events.
 * <p>
 * Immutable.
 */
public class CollectionsManifest {

    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * A manifest with just the default scope and default collection.
     */
    public static final CollectionsManifest DEFAULT = defaultManifest();

    /**
     * A manifest with no collections & no scopes
     */
    public static final CollectionsManifest EMPTY_MANIFEST =
            new CollectionsManifest(0, Int2ObjectMaps.emptyMap(), Int2ObjectMaps.emptyMap());

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static class ScopeInfo {
        public static final String DEFAULT_NAME = "_default";

        private final int id;
        private final String name;
        private final Map<String, CollectionInfo> collectionsByName;

        public ScopeInfo(int id, String name, Map<String, CollectionInfo> collectionsByName) {
            this.id = id;
            this.name = requireNonNull(name);
            this.collectionsByName = Collections.unmodifiableMap(collectionsByName);
        }

        public int id() {
            return id;
        }

        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ScopeInfo scopeInfo = (ScopeInfo) o;
            return id == scopeInfo.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public String toString() {
            return "0x" + Integer.toUnsignedString(id, 16) + ":" + name;
        }
    }

    public static class CollectionInfo {
        public static final String DEFAULT_NAME = "_default";
        public static final long MAX_TTL_UNDEFINED = -1L;
        private final ScopeInfo scope;
        private final int id;
        private final String name;
        private final long maxTtl;

        public CollectionInfo(ScopeInfo scope, int id, String name, long maxTtl) {
            this.id = id;
            this.name = requireNonNull(name);
            this.scope = requireNonNull(scope);
            this.maxTtl = maxTtl;
        }

        public CollectionInfo(ScopeInfo scope, int id, String name) {
            this(scope, id, name, MAX_TTL_UNDEFINED);
        }

        public ScopeInfo scope() {
            return scope;
        }

        public int id() {
            return id;
        }

        public String name() {
            return name;
        }

        public long maxTtl() {
            return maxTtl;
        }

        @Override
        public String toString() {
            return "CollectionInfo{" + "id=" + CollectionsUtil.displayCid(id) + ", name='" + name + '\'' + ", scope="
                    + scope + ", maxTtl=" + (maxTtl == MAX_TTL_UNDEFINED ? "<UNDEFINED>" : maxTtl) + '}';
        }
    }

    private final long uid;
    private final Int2ObjectMap<ScopeInfo> scopesById;
    private final Int2ObjectMap<CollectionInfo> collectionsById;

    private static <T> Int2ObjectMap<T> copyToUnmodifiableMap(Int2ObjectMap<T> map) {
        return Int2ObjectMaps.unmodifiable(new Int2ObjectOpenHashMap<>(map));
    }

    private CollectionsManifest(long manifestUid, Int2ObjectMap<ScopeInfo> scopesById,
            Int2ObjectMap<CollectionInfo> collectionsById) {
        this.uid = manifestUid;
        this.scopesById = copyToUnmodifiableMap(scopesById);
        this.collectionsById = copyToUnmodifiableMap(collectionsById);
    }

    private static CollectionsManifest defaultManifest() {
        CollectionsManifest defaultManifest =
                new CollectionsManifest(0, Int2ObjectMaps.emptyMap(), Int2ObjectMaps.emptyMap());
        defaultManifest = defaultManifest.withScope(0, 0, ScopeInfo.DEFAULT_NAME);
        return defaultManifest.withCollection(0, 0, 0, CollectionInfo.DEFAULT_NAME, CollectionInfo.MAX_TTL_UNDEFINED);
    }

    public CollectionsManifest withManifestId(long newManifestUid) {
        return new CollectionsManifest(newManifestUid, scopesById, collectionsById);
    }

    public CollectionsManifest withScope(long newManifestUid, int newScopeId, String newScopeName) {
        Int2ObjectMap<ScopeInfo> newScopeMap = new Int2ObjectAVLTreeMap<>(scopesById);
        newScopeMap.put(newScopeId, new ScopeInfo(newScopeId, newScopeName, new HashMap<>()));
        return new CollectionsManifest(newManifestUid, newScopeMap, collectionsById);
    }

    public CollectionsManifest withoutScope(long newManifestUid, int doomedScopeId) {
        Int2ObjectMap<ScopeInfo> newScopeMap = new Int2ObjectAVLTreeMap<>(scopesById);

        Int2ObjectMap<CollectionInfo> newCollectionMap = collectionsById.int2ObjectEntrySet().stream()
                .filter(e -> e.getValue().scope().id() != doomedScopeId).collect(Collectors.toInt2ObjectMap());

        return new CollectionsManifest(newManifestUid, newScopeMap, newCollectionMap);
    }

    public CollectionsManifest withCollection(long newManifestUid, int scopeId, int collectionId, String collectionName,
            long maxTtl) {
        final ScopeInfo scopeInfo = scopesById.get(scopeId);
        if (scopeInfo == null) {
            throw new IllegalStateException("Unrecognized scope id: " + scopeId);
        }
        final CollectionInfo collectionInfo = new CollectionInfo(scopeInfo, collectionId, collectionName, maxTtl);

        Map<String, CollectionInfo> newCollectionsByName = new HashMap<>(scopeInfo.collectionsByName);
        newCollectionsByName.put(collectionName, collectionInfo);

        final ScopeInfo newScopeInfo = new ScopeInfo(scopeInfo.id, scopeInfo.name, newCollectionsByName);

        Int2ObjectMap<ScopeInfo> newScopeMap = new Int2ObjectAVLTreeMap<>(scopesById);
        newScopeMap.put(scopeId, newScopeInfo);

        Int2ObjectMap<CollectionInfo> newCollectionMap = new Int2ObjectAVLTreeMap<>(collectionsById);
        newCollectionMap.put(collectionId, collectionInfo);

        return new CollectionsManifest(newManifestUid, newScopeMap, newCollectionMap);
    }

    public CollectionsManifest withoutCollection(long newManifestUid, int id) {
        Int2ObjectMap<CollectionInfo> result;
        CollectionInfo collectionInfo = collectionsById.get(id);
        if (collectionInfo == null) {
            LOGGER.debug("can't remove collection id: " + id + " as it was not found");
            if (uid == newManifestUid) {
                return this;
            }
            return new CollectionsManifest(newManifestUid, scopesById, collectionsById);
        } else {
            result = collectionsById.int2ObjectEntrySet().stream().filter(e -> e.getValue().id() != id)
                    .collect(Collectors.toInt2ObjectMap());
        }
        final ScopeInfo scopeInfo = collectionInfo.scope;
        Map<String, CollectionInfo> newCollectionsByName = new HashMap<>(scopeInfo.collectionsByName);
        newCollectionsByName.remove(collectionInfo.name);
        final ScopeInfo newScopeInfo = new ScopeInfo(scopeInfo.id, scopeInfo.name, newCollectionsByName);

        Int2ObjectMap<ScopeInfo> newScopeMap = new Int2ObjectAVLTreeMap<>(scopesById);
        newScopeMap.put(scopeInfo.id, newScopeInfo);

        return new CollectionsManifest(newManifestUid, newScopeMap, result);
    }

    public CollectionInfo getCollection(int id) {
        return collectionsById.get(id);
    }

    public CollectionInfo getCollection(ScopeInfo scope, String collectionName) {
        return scope.collectionsByName.get(collectionName);
    }

    public ScopeInfo getScope(String name) {
        return scopesById.values().stream().filter(s -> s.name().equals(name)).findFirst().orElse(null);
    }

    public CollectionInfo getCollection(String scopeName, String collectionName) {
        ScopeInfo scope = getScope(scopeName);
        return scope == null ? null : getCollection(scope, collectionName);
    }

    public Stream<ScopeInfo> stream() {
        return scopesById.values().stream();
    }

    public long getUid() {
        return uid;
    }

    @Override
    public String toString() {
        return "CollectionsManifest{" + "uid=0x" + Long.toUnsignedString(uid, 16) + '}';
    }

    public String toDetailedString() {
        return "CollectionsManifest{" + "uid=0x" + Long.toUnsignedString(uid, 16) + ", scopes=" + scopesById.values()
                + ", collections=" + collectionsById.values() + '}';
    }

    public static CollectionsManifest fromJson(byte[] jsonBytes) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("fromJson: {}", new String(jsonBytes, StandardCharsets.UTF_8));
        }
        ManifestJson manifestBinder = OBJECT_MAPPER.readValue(jsonBytes, ManifestJson.class);
        return manifestBinder.build(); // crazy inefficient, with all the map copying.
    }

    public byte[] toJson() throws IOException {
        ManifestJson result = new ManifestJson();
        result.uid = Long.toUnsignedString(uid, 16);
        result.scopes = this.stream().map(ScopeJson::from).collect(java.util.stream.Collectors.toList());
        return OBJECT_MAPPER.writeValueAsBytes(result);
    }

    private static long parseId64(String id) {
        return Long.parseUnsignedLong(id, 16);
    }

    private static int parseId32(String id) {
        return Integer.parseUnsignedInt(id, 16);
    }

    @SuppressWarnings("WeakerAccess")
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ManifestJson {
        public String uid;
        public List<ScopeJson> scopes;

        private CollectionsManifest build() {
            long manifestUid = parseId64(uid);
            CollectionsManifest m =
                    new CollectionsManifest(manifestUid, Int2ObjectMaps.emptyMap(), Int2ObjectMaps.emptyMap());
            for (ScopeJson s : scopes) {
                m = s.build(m, manifestUid);
            }
            return m;
        }
    }

    @SuppressWarnings("WeakerAccess")
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ScopeJson {
        public String uid;
        public String name;
        public List<CollectionJson> collections;

        private static ScopeJson from(ScopeInfo info) {
            ScopeJson result = new ScopeJson();
            result.uid = Integer.toUnsignedString(info.id, 16);
            result.name = info.name;
            result.collections = info.collectionsByName.values().stream().map(collectionInfo -> {
                CollectionJson collectionResult = new CollectionJson();
                collectionResult.uid = CollectionsUtil.encodeCid(collectionInfo.id);
                collectionResult.max_ttl = collectionInfo.maxTtl;
                collectionResult.name = collectionInfo.name;
                return collectionResult;
            }).collect(java.util.stream.Collectors.toList());
            return result;
        }

        private CollectionsManifest build(CollectionsManifest m, long manifestId) {
            int scopeId = parseId32(uid);
            m = m.withScope(manifestId, scopeId, name);
            for (CollectionJson c : collections) {
                m = c.build(m, manifestId, scopeId);
            }
            return m;
        }
    }

    @SuppressWarnings("WeakerAccess")
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class CollectionJson {
        public String uid;
        public String name;
        public long max_ttl;

        private CollectionsManifest build(CollectionsManifest m, long manifestId, int scopeId) {
            return m.withCollection(manifestId, scopeId, parseId32(uid), name, max_ttl);
        }
    }

}
