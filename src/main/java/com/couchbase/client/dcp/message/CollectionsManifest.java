/*
 * Copyright 2020 Couchbase, Inc.
 */

package com.couchbase.client.dcp.message;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hyracks.util.fastutil.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    /**
     * A manifest with just the default scope and default collection.
     */
    public static final CollectionsManifest DEFAULT = defaultManifest();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static class ScopeInfo {
        public static final ScopeInfo DEFAULT = new ScopeInfo(0, "_default");

        private final int id;
        private final String name;

        public ScopeInfo(int id, String name) {
            this.id = id;
            this.name = requireNonNull(name);
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
            return "CollectionInfo{" + "scope=" + scope + ", id=0x" + Integer.toUnsignedString(id, 16) + ", name='"
                    + name + '\'' + ", maxTtl=" + (maxTtl == MAX_TTL_UNDEFINED ? "<UNDEFINED>" : maxTtl) + '}';
        }
    }

    private final long id;
    private final Int2ObjectMap<ScopeInfo> scopesById;
    private final Int2ObjectMap<CollectionInfo> collectionsById;

    private static <T> Int2ObjectMap<T> copyToUnmodifiableMap(Int2ObjectMap<T> map) {
        return Int2ObjectMaps.unmodifiable(new Int2ObjectOpenHashMap<>(map));
    }

    private CollectionsManifest(long manifestId, Int2ObjectMap<ScopeInfo> scopesById,
            Int2ObjectMap<CollectionInfo> collectionsById) {
        this.id = manifestId;
        this.scopesById = copyToUnmodifiableMap(scopesById);
        this.collectionsById = copyToUnmodifiableMap(collectionsById);
    }

    private static CollectionsManifest defaultManifest() {
        ScopeInfo defaultScope = ScopeInfo.DEFAULT;
        ScopeInfo defaultCollection = ScopeInfo.DEFAULT;
        return new CollectionsManifest(0, Int2ObjectMaps.singleton(defaultScope.id(), defaultScope),
                Int2ObjectMaps.singleton(defaultCollection.id(),
                        new CollectionInfo(defaultScope, defaultCollection.id(), defaultCollection.name())));
    }

    public CollectionsManifest withManifestId(long newManifestId) {
        return new CollectionsManifest(newManifestId, scopesById, collectionsById);
    }

    public CollectionsManifest withScope(long newManifestId, int newScopeId, String newScopeName) {
        Int2ObjectMap<ScopeInfo> newScopeMap = new Int2ObjectOpenHashMap<>(scopesById);
        newScopeMap.put(newScopeId, new ScopeInfo(newScopeId, newScopeName));
        return new CollectionsManifest(newManifestId, newScopeMap, collectionsById);
    }

    public CollectionsManifest withoutScope(long newManifestId, int doomedScopeId) {
        Int2ObjectMap<ScopeInfo> newScopeMap = new Int2ObjectOpenHashMap<>(scopesById);
        newScopeMap.remove(doomedScopeId);

        Int2ObjectMap<CollectionInfo> newCollectionMap = collectionsById.int2ObjectEntrySet().stream()
                .filter(e -> e.getValue().scope().id() != doomedScopeId).collect(Collectors.toInt2ObjectMap());

        return new CollectionsManifest(newManifestId, newScopeMap, newCollectionMap);
    }

    public CollectionsManifest withCollection(long newManifestId, int scopeId, int collectionId, String collectionName,
            long maxTtl) {
        final ScopeInfo scopeIdAndName = scopesById.get(scopeId);
        if (scopeIdAndName == null) {
            throw new IllegalStateException("Unrecognized scope ID: " + scopeId);
        }
        Int2ObjectMap<CollectionInfo> newCollectionMap = new Int2ObjectOpenHashMap<>(collectionsById);
        newCollectionMap.put(collectionId, new CollectionInfo(scopeIdAndName, collectionId, collectionName, maxTtl));

        return new CollectionsManifest(newManifestId, scopesById, newCollectionMap);
    }

    public CollectionsManifest withoutCollection(long newManifestId, int id) {
        Int2ObjectMap<CollectionInfo> result = collectionsById.int2ObjectEntrySet().stream()
                .filter(e -> e.getValue().id() != id).collect(Collectors.toInt2ObjectMap());

        return new CollectionsManifest(newManifestId, scopesById, result);
    }

    public CollectionInfo getCollection(int id) {
        return collectionsById.get(id);
    }

    public CollectionInfo getCollection(ScopeInfo scope, String collectionName) {
        return collectionsById.values().stream().filter(c -> c.name().equals(collectionName))
                .filter(c -> c.scope().equals(scope)).findFirst().orElse(null);
    }

    /**
     * @param name A fully-qualified collection name like "myScope.myCollection"
     */
    public CollectionInfo getCollection(String name) {
        String[] split = name.split("\\.", -1);
        if (split.length != 2) {
            throw new IllegalArgumentException(
                    "Collection name must be qualified by scope, like: myScope.myCollection");
        }

        String scope = split[0];
        String collection = split[1];

        return collectionsById.values().stream().filter(c -> c.name().equals(collection))
                .filter(c -> c.scope().name().equals(scope)).findFirst().orElse(null);
    }

    public ScopeInfo getScope(String name) {
        return scopesById.values().stream().filter(s -> s.name().equals(name)).findFirst().orElse(null);
    }

    public Stream<ScopeInfo> stream() {
        return scopesById.values().stream();
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "CollectionsManifest{" + "id=0x" + Long.toUnsignedString(id, 16) + ", scopesById=" + scopesById
                + ", collectionsById=" + collectionsById + '}';
    }

    public static CollectionsManifest fromJson(byte[] jsonBytes) throws IOException {
        ManifestJson manifestBinder = OBJECT_MAPPER.readValue(jsonBytes, ManifestJson.class);
        return manifestBinder.build(); // crazy inefficient, with all the map copying.
    }

    private static int parseId(String id) {
        return Integer.parseUnsignedInt(id, 16);
    }

    @SuppressWarnings("WeakerAccess")
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ManifestJson {
        public String uid;
        public List<ScopeJson> scopes;

        private CollectionsManifest build() {
            int manifestId = parseId(uid);
            CollectionsManifest m =
                    new CollectionsManifest(manifestId, Int2ObjectMaps.emptyMap(), Int2ObjectMaps.emptyMap());
            for (ScopeJson s : scopes) {
                m = s.build(m, manifestId);
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

        private CollectionsManifest build(CollectionsManifest m, int manifestId) {
            int scopeId = parseId(uid);
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

        private CollectionsManifest build(CollectionsManifest m, int manifestId, int scopeId) {
            return m.withCollection(manifestId, scopeId, parseId(uid), name, max_ttl);
        }
    }

}
