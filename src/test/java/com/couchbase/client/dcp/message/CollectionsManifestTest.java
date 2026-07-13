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
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.TEST_GENERATED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.GITHUB_COPILOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Test;

@AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = TEST_GENERATED)
public class CollectionsManifestTest {

    @Test
    public void testDefaultManifest() {
        CollectionsManifest manifest = CollectionsManifest.DEFAULT;
        assertEquals(0, manifest.getUid());

        // default scope exists
        CollectionsManifest.ScopeInfo defaultScope = manifest.getScope("_default");
        assertNotNull(defaultScope);
        assertEquals(0, defaultScope.id());
        assertEquals("_default", defaultScope.name());

        // default collection exists within default scope
        CollectionsManifest.CollectionInfo defaultCollection = manifest.getCollection("_default", "_default");
        assertNotNull(defaultCollection);
        assertEquals(0, defaultCollection.id());
        assertEquals("_default", defaultCollection.name());
        assertEquals(defaultScope, defaultCollection.scope());
        assertEquals(CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED, defaultCollection.maxTtl());
    }

    @Test
    public void testEmptyManifest() {
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        assertEquals(0, manifest.getUid());
        assertNull(manifest.getScope("_default"));
        assertNull(manifest.getCollection(0));
        assertNull(manifest.getCollection("_default", "_default"));
    }

    @Test
    public void testTrivialManifestFromJson() throws IOException {
        // A manifest with just the default scope and default collection
        String json = "{\"uid\":\"0\",\"scopes\":[{\"uid\":\"0\",\"name\":\"_default\","
                + "\"collections\":[{\"uid\":\"0\",\"name\":\"_default\",\"max_ttl\":0}]}]}";
        CollectionsManifest manifest = CollectionsManifest.fromJson(json.getBytes());
        assertEquals(0, manifest.getUid());

        CollectionsManifest.ScopeInfo scope = manifest.getScope("_default");
        assertNotNull(scope);
        assertEquals(0, scope.id());

        CollectionsManifest.CollectionInfo col = manifest.getCollection("_default", "_default");
        assertNotNull(col);
        assertEquals(0, col.id());
    }

    @Test
    public void testWithScopeAndCollection() {
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, 1, "myScope");

        CollectionsManifest.ScopeInfo scope = manifest.getScope("myScope");
        assertNotNull(scope);
        assertEquals(1, scope.id());

        manifest = manifest.withCollection(2, 1, 100, "myCollection",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        CollectionsManifest.CollectionInfo col = manifest.getCollection(100);
        assertNotNull(col);
        assertEquals("myCollection", col.name());
        assertEquals(scope.id(), col.scope().id());
        assertEquals(2, manifest.getUid());
    }

    @Test
    public void testWithoutCollection() {
        CollectionsManifest manifest = CollectionsManifest.DEFAULT;
        // Remove the default collection (id=0)
        manifest = manifest.withoutCollection(1, 0);
        assertEquals(1, manifest.getUid());
        assertNull(manifest.getCollection(0));
        // Scope should still exist
        assertNotNull(manifest.getScope("_default"));
    }

    @Test
    public void testWithoutScope() {
        CollectionsManifest manifest = CollectionsManifest.DEFAULT;
        // Remove the default scope (id=0), which should also remove its collections
        manifest = manifest.withoutScope(1, 0);
        assertEquals(1, manifest.getUid());
        // Collections under that scope should be gone
        assertNull(manifest.getCollection(0));
    }

    @Test
    public void testManifestIdUpdate() {
        CollectionsManifest manifest = CollectionsManifest.DEFAULT;
        assertEquals(0, manifest.getUid());
        manifest = manifest.withManifestId(42);
        assertEquals(42, manifest.getUid());
        // Existing data intact
        assertNotNull(manifest.getCollection(0));
        assertNotNull(manifest.getScope("_default"));
    }

    @Test
    public void testJsonRoundTrip() throws IOException {
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, 1, "scope1");
        manifest = manifest.withCollection(1, 1, 10, "col1", 3600);
        manifest = manifest.withScope(1, 2, "scope2");
        manifest = manifest.withCollection(1, 2, 20, "col2", CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        byte[] jsonBytes = manifest.toJson();
        CollectionsManifest restored = CollectionsManifest.fromJson(jsonBytes);

        assertEquals(manifest.getUid(), restored.getUid());
        assertNotNull(restored.getScope("scope1"));
        assertNotNull(restored.getScope("scope2"));
        assertNotNull(restored.getCollection("scope1", "col1"));
        assertNotNull(restored.getCollection("scope2", "col2"));
        assertEquals(10, restored.getCollection("scope1", "col1").id());
        assertEquals(20, restored.getCollection("scope2", "col2").id());
        assertEquals(3600, restored.getCollection("scope1", "col1").maxTtl());
    }

    @Test
    public void test10000CollectionsSingleScope() {
        // Build a manifest with 1 scope and 10000 collections
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(0, 1, "bigScope");

        for (int i = 1; i <= 10000; i++) {
            manifest = manifest.withCollection(i, 1, i, "collection_" + i,
                    CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
        }

        assertEquals(10000, manifest.getUid());

        // Verify first, middle, and last
        CollectionsManifest.CollectionInfo first = manifest.getCollection(1);
        assertNotNull(first);
        assertEquals("collection_1", first.name());
        assertEquals(1, first.scope().id());

        CollectionsManifest.CollectionInfo mid = manifest.getCollection(5000);
        assertNotNull(mid);
        assertEquals("collection_5000", mid.name());

        CollectionsManifest.CollectionInfo last = manifest.getCollection(10000);
        assertNotNull(last);
        assertEquals("collection_10000", last.name());

        // Verify scope lookup
        CollectionsManifest.ScopeInfo scope = manifest.getScope("bigScope");
        assertNotNull(scope);
        assertEquals(1, scope.id());
    }

    @Test
    public void test10000CollectionsDistributedAcross10Scopes() {
        // 10 scopes, each with 1000 collections = 10000 total
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        int collectionId = 1;
        for (int s = 1; s <= 10; s++) {
            manifest = manifest.withScope(collectionId, s, "scope_" + s);
            for (int c = 0; c < 1000; c++) {
                manifest = manifest.withCollection(collectionId, s, collectionId, "col_" + collectionId,
                        CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
                collectionId++;
            }
        }

        // Verify all 10 scopes
        for (int s = 1; s <= 10; s++) {
            assertNotNull(manifest.getScope("scope_" + s));
        }

        // Verify random collections from different scopes
        CollectionsManifest.CollectionInfo col1 = manifest.getCollection(1);
        assertNotNull(col1);
        assertEquals("scope_1", col1.scope().name());

        CollectionsManifest.CollectionInfo col5001 = manifest.getCollection(5001);
        assertNotNull(col5001);
        assertEquals("scope_6", col5001.scope().name());

        CollectionsManifest.CollectionInfo col10000 = manifest.getCollection(10000);
        assertNotNull(col10000);
        assertEquals("scope_10", col10000.scope().name());
    }

    @Test
    public void test10000CollectionsDistributedAcross100Scopes() {
        // 100 scopes, each with 100 collections = 10000 total
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        int collectionId = 1;
        for (int s = 1; s <= 100; s++) {
            manifest = manifest.withScope(collectionId, s, "scope_" + s);
            for (int c = 0; c < 100; c++) {
                manifest = manifest.withCollection(collectionId, s, collectionId, "col_" + collectionId,
                        CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
                collectionId++;
            }
        }

        // Verify scope count
        for (int s = 1; s <= 100; s++) {
            assertNotNull(manifest.getScope("scope_" + s));
        }

        // Spot checks
        CollectionsManifest.CollectionInfo col50 = manifest.getCollection(50);
        assertNotNull(col50);
        assertEquals("scope_1", col50.scope().name());

        CollectionsManifest.CollectionInfo col5050 = manifest.getCollection(5050);
        assertNotNull(col5050);
        assertEquals("scope_51", col5050.scope().name());
    }

    @Test
    public void test10000CollectionsDistributedAcross1000Scopes() {
        // 1000 scopes, each with 10 collections = 10000 total
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        int collectionId = 1;
        for (int s = 1; s <= 1000; s++) {
            manifest = manifest.withScope(collectionId, s, "scope_" + s);
            for (int c = 0; c < 10; c++) {
                manifest = manifest.withCollection(collectionId, s, collectionId, "col_" + collectionId,
                        CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
                collectionId++;
            }
        }

        // Verify scope count
        for (int s = 1; s <= 1000; s++) {
            assertNotNull(manifest.getScope("scope_" + s));
        }

        // Spot check first scope's collections
        for (int i = 1; i <= 10; i++) {
            CollectionsManifest.CollectionInfo col = manifest.getCollection(i);
            assertNotNull(col);
            assertEquals("scope_1", col.scope().name());
        }

        // Spot check last scope's collections
        for (int i = 9991; i <= 10000; i++) {
            CollectionsManifest.CollectionInfo col = manifest.getCollection(i);
            assertNotNull(col);
            assertEquals("scope_1000", col.scope().name());
        }
    }

    @Test
    public void test10000ScopesOneCollectionEach() {
        // Most pathological: 10000 scopes, each with exactly 1 collection
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        for (int s = 1; s <= 10000; s++) {
            manifest = manifest.withScope(s, s, "scope_" + s);
            manifest = manifest.withCollection(s, s, s, "col_in_scope_" + s,
                    CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
        }

        assertEquals(10000, manifest.getUid());

        // Verify all scopes exist
        for (int s = 1; s <= 10000; s++) {
            CollectionsManifest.ScopeInfo scope = manifest.getScope("scope_" + s);
            assertNotNull("scope_" + s + " should exist", scope);
            assertEquals(s, scope.id());
        }

        // Verify all collections and their scope associations
        for (int s = 1; s <= 10000; s++) {
            CollectionsManifest.CollectionInfo col = manifest.getCollection(s);
            assertNotNull("Collection " + s + " should exist", col);
            assertEquals("col_in_scope_" + s, col.name());
            assertEquals(s, col.scope().id());
            assertEquals("scope_" + s, col.scope().name());
        }

        // Verify lookup by scope and collection name
        CollectionsManifest.CollectionInfo first = manifest.getCollection("scope_1", "col_in_scope_1");
        assertNotNull(first);
        assertEquals(1, first.id());

        CollectionsManifest.CollectionInfo last = manifest.getCollection("scope_10000", "col_in_scope_10000");
        assertNotNull(last);
        assertEquals(10000, last.id());
    }

    @Test
    public void test10000ScopesOneCollectionEachJsonRoundTrip() throws IOException {
        // Build 10000 scopes with 1 collection each, serialize and deserialize
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        for (int s = 1; s <= 10000; s++) {
            manifest = manifest.withScope(s, s, "scope_" + s);
            manifest =
                    manifest.withCollection(s, s, s, "col_" + s, CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
        }

        byte[] jsonBytes = manifest.toJson();
        CollectionsManifest restored = CollectionsManifest.fromJson(jsonBytes);

        assertEquals(manifest.getUid(), restored.getUid());

        // Spot checks after round-trip
        assertNotNull(restored.getScope("scope_1"));
        assertNotNull(restored.getScope("scope_5000"));
        assertNotNull(restored.getScope("scope_10000"));

        CollectionsManifest.CollectionInfo col1 = restored.getCollection(1);
        assertNotNull(col1);
        assertEquals("col_1", col1.name());
        assertEquals("scope_1", col1.scope().name());

        CollectionsManifest.CollectionInfo col10000 = restored.getCollection(10000);
        assertNotNull(col10000);
        assertEquals("col_10000", col10000.name());
        assertEquals("scope_10000", col10000.scope().name());
    }

    @Test
    public void test10000CollectionsSingleScopeJsonRoundTrip() throws IOException {
        // Build 1 scope with 10000 collections, serialize and deserialize
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(0, 1, "megaScope");
        for (int i = 1; i <= 10000; i++) {
            manifest =
                    manifest.withCollection(i, 1, i, "col_" + i, CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
        }

        byte[] jsonBytes = manifest.toJson();
        CollectionsManifest restored = CollectionsManifest.fromJson(jsonBytes);

        assertEquals(manifest.getUid(), restored.getUid());
        assertNotNull(restored.getScope("megaScope"));

        // Spot checks
        for (int i = 1; i <= 10000; i += 1000) {
            CollectionsManifest.CollectionInfo col = restored.getCollection(i);
            assertNotNull("Collection " + i + " should survive round-trip", col);
            assertEquals("col_" + i, col.name());
            assertEquals("megaScope", col.scope().name());
        }
    }

    @Test
    public void testWithCollectionMaxTtl() {
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, 1, "ttlScope");
        manifest = manifest.withCollection(1, 1, 10, "ttlCollection", 7200);

        CollectionsManifest.CollectionInfo col = manifest.getCollection(10);
        assertNotNull(col);
        assertEquals(7200, col.maxTtl());
    }

    @Test
    public void testRemoveNonExistentCollection() {
        CollectionsManifest manifest = CollectionsManifest.DEFAULT;
        // Removing a non-existent collection should not throw, manifest uid updates
        CollectionsManifest result = manifest.withoutCollection(5, 9999);
        assertEquals(5, result.getUid());
        // Default collection still intact
        assertNotNull(result.getCollection(0));
    }

    @Test
    public void testIncrementalGrowthToManyCollections() {
        // Simulate incremental growth: add collections one by one and verify state at milestones
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(0, 1, "growScope");

        int[] milestones = { 1, 10, 100, 1000, 5000, 10000 };
        int milestoneIdx = 0;

        for (int i = 1; i <= 10000; i++) {
            manifest = manifest.withCollection(i, 1, i, "c_" + i, CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

            if (milestoneIdx < milestones.length && i == milestones[milestoneIdx]) {
                // Verify all collections up to this point are accessible
                for (int j = 1; j <= i; j++) {
                    assertNotNull("At milestone " + i + ", collection " + j + " should exist",
                            manifest.getCollection(j));
                }
                milestoneIdx++;
            }
        }

        // Final verification
        assertEquals(10000, manifest.getUid());
        for (int i = 1; i <= 10000; i++) {
            assertNotNull(manifest.getCollection(i));
        }
    }

    @Test
    public void testMultipleScopesWithRemoval() {
        // Create 5 scopes with 2000 collections each, then remove some
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        int collectionId = 1;
        for (int s = 1; s <= 5; s++) {
            manifest = manifest.withScope(collectionId, s, "scope_" + s);
            for (int c = 0; c < 2000; c++) {
                manifest = manifest.withCollection(collectionId, s, collectionId, "col_" + collectionId,
                        CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
                collectionId++;
            }
        }

        // Remove 100 collections from scope_3 (ids 4001..4100)
        for (int i = 4001; i <= 4100; i++) {
            manifest = manifest.withoutCollection(collectionId++, i);
        }

        // Those collections should be gone
        for (int i = 4001; i <= 4100; i++) {
            assertNull("Collection " + i + " should have been removed", manifest.getCollection(i));
        }

        // But others in scope_3 remain
        for (int i = 4101; i <= 5000; i++) {
            assertNotNull("Collection " + i + " should still exist", manifest.getCollection(i));
        }

        // Other scopes unaffected
        assertNotNull(manifest.getCollection(1)); // scope_1
        assertNotNull(manifest.getCollection(9000)); // scope_5
    }

    // -- Tests for unsigned CIDs and manifest UIDs beyond signed maximums --

    @Test
    public void testUnsignedCollectionId() {
        // CID > Integer.MAX_VALUE (0x7FFFFFFF) — exercises unsigned 32-bit range
        int unsignedCid = 0x80000000; // 2^31, negative as signed int
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, 1, "scope1");
        manifest = manifest.withCollection(1, 1, unsignedCid, "highCollection",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        CollectionsManifest.CollectionInfo col = manifest.getCollection(unsignedCid);
        assertNotNull(col);
        assertEquals(unsignedCid, col.id());
        assertEquals("highCollection", col.name());
        assertEquals("scope1", col.scope().name());
    }

    @Test
    public void testUnsignedCollectionIdMaxValue() {
        // CID = 0xFFFFFFFF — maximum unsigned 32-bit value
        int maxUnsignedCid = 0xFFFFFFFF; // -1 as signed int
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, 1, "scope1");
        manifest = manifest.withCollection(1, 1, maxUnsignedCid, "maxCollection",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        CollectionsManifest.CollectionInfo col = manifest.getCollection(maxUnsignedCid);
        assertNotNull(col);
        assertEquals(maxUnsignedCid, col.id());
        assertEquals("maxCollection", col.name());
    }

    @Test
    public void testUnsignedScopeId() {
        // Scope ID > Integer.MAX_VALUE
        int unsignedScopeId = 0xDEADBEEF; // negative as signed int
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, unsignedScopeId, "unsignedScope");
        manifest = manifest.withCollection(1, unsignedScopeId, 42, "col42",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        CollectionsManifest.ScopeInfo scope = manifest.getScope("unsignedScope");
        assertNotNull(scope);
        assertEquals(unsignedScopeId, scope.id());

        CollectionsManifest.CollectionInfo col = manifest.getCollection(42);
        assertNotNull(col);
        assertEquals(unsignedScopeId, col.scope().id());
    }

    @Test
    public void testUnsignedManifestUid() {
        // Manifest UID > Long.MAX_VALUE (0x7FFFFFFFFFFFFFFF)
        long unsignedManifestUid = 0x8000000000000000L; // 2^63, negative as signed long
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(unsignedManifestUid, 1, "scope1");

        assertEquals(unsignedManifestUid, manifest.getUid());
    }

    @Test
    public void testMaxUnsignedManifestUid() {
        // Manifest UID = 0xFFFFFFFFFFFFFFFF — maximum unsigned 64-bit value
        long maxUnsignedUid = 0xFFFFFFFFFFFFFFFFL; // -1 as signed long
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(maxUnsignedUid, 1, "scope1");
        manifest = manifest.withCollection(maxUnsignedUid, 1, 100, "col100",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        assertEquals(maxUnsignedUid, manifest.getUid());
        assertNotNull(manifest.getCollection(100));
    }

    @Test
    public void testUnsignedIdsJsonRoundTrip() throws IOException {
        // Build manifest with unsigned CIDs and scope IDs, verify round-trip
        int unsignedScopeId = 0xCAFEBABE;
        int unsignedCid1 = 0x80000001;
        int unsignedCid2 = 0xFFFFFFFF;
        long unsignedManifestUid = 0xABCDEF0123456789L;

        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(unsignedManifestUid, unsignedScopeId, "cafeScope");
        manifest = manifest.withCollection(unsignedManifestUid, unsignedScopeId, unsignedCid1, "col_high",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
        manifest = manifest.withCollection(unsignedManifestUid, unsignedScopeId, unsignedCid2, "col_max", 7200);

        // Serialize
        byte[] jsonBytes = manifest.toJson();

        // Verify JSON contains correct hex representations
        String jsonStr = new String(jsonBytes);
        // manifest uid should be the hex of 0xABCDEF0123456789
        assertTrue(jsonStr.contains("abcdef0123456789"));
        // scope uid should be hex of 0xCAFEBABE
        assertTrue(jsonStr.contains("cafebabe"));
        // collection uids
        assertTrue(jsonStr.contains("80000001"));
        assertTrue(jsonStr.contains("ffffffff"));

        // Deserialize and verify
        CollectionsManifest restored = CollectionsManifest.fromJson(jsonBytes);
        assertEquals(unsignedManifestUid, restored.getUid());

        CollectionsManifest.ScopeInfo scope = restored.getScope("cafeScope");
        assertNotNull(scope);
        assertEquals(unsignedScopeId, scope.id());

        CollectionsManifest.CollectionInfo col1 = restored.getCollection(unsignedCid1);
        assertNotNull(col1);
        assertEquals(unsignedCid1, col1.id());
        assertEquals("col_high", col1.name());
        assertEquals(unsignedScopeId, col1.scope().id());

        CollectionsManifest.CollectionInfo col2 = restored.getCollection(unsignedCid2);
        assertNotNull(col2);
        assertEquals(unsignedCid2, col2.id());
        assertEquals("col_max", col2.name());
        assertEquals(7200, col2.maxTtl());
    }

    @Test
    public void testUnsignedCidRemoval() {
        // Verify withoutCollection works with unsigned CIDs
        int unsignedCid = 0xDEAD0000;
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, 1, "scope1");
        manifest = manifest.withCollection(1, 1, unsignedCid, "deadCollection",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        assertNotNull(manifest.getCollection(unsignedCid));

        manifest = manifest.withoutCollection(2, unsignedCid);
        assertNull(manifest.getCollection(unsignedCid));
        assertEquals(2, manifest.getUid());
    }

    @Test
    public void testUnsignedScopeIdRemoval() {
        // Verify withoutScope works with unsigned scope IDs
        int unsignedScopeId = 0xBAADF00D;
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, unsignedScopeId, "baadScope");
        manifest = manifest.withCollection(1, unsignedScopeId, 1, "col1",
                CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);

        assertNotNull(manifest.getScope("baadScope"));
        assertNotNull(manifest.getCollection(1));

        manifest = manifest.withoutScope(2, unsignedScopeId);
        assertNull(manifest.getScope("baadScope"));
        assertNull(manifest.getCollection(1));
    }

    @Test
    public void testMultipleUnsignedCidsInSameScope() {
        // Multiple collections with CIDs in the upper unsigned range
        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(1, 1, "scope1");

        int[] unsignedCids = { 0x80000000, 0x90000000, 0xA0000000, 0xB0000000, 0xC0000000, 0xD0000000, 0xE0000000,
                0xF0000000, 0xFFFFFFFE, 0xFFFFFFFF };

        for (int unsignedCid : unsignedCids) {
            manifest = manifest.withCollection(1, 1, unsignedCid, "col_" + Integer.toUnsignedString(unsignedCid, 16),
                    CollectionsManifest.CollectionInfo.MAX_TTL_UNDEFINED);
        }

        // Verify all are accessible
        for (int unsignedCid : unsignedCids) {
            CollectionsManifest.CollectionInfo col = manifest.getCollection(unsignedCid);
            assertNotNull("CID 0x" + Integer.toUnsignedString(unsignedCid, 16) + " should be found", col);
            assertEquals(unsignedCid, col.id());
            assertEquals("col_" + Integer.toUnsignedString(unsignedCid, 16), col.name());
        }

        // Verify lookup by name works too
        CollectionsManifest.CollectionInfo maxCol = manifest.getCollection("scope1", "col_ffffffff");
        assertNotNull(maxCol);
        assertEquals(0xFFFFFFFF, maxCol.id());
    }

    @Test
    public void testUnsignedManifestUidProgression() {
        // Simulate manifest UID crossing the signed boundary
        long justBelowSignedMax = Long.MAX_VALUE; // 0x7FFFFFFFFFFFFFFF
        long atUnsignedBoundary = 0x8000000000000000L; // 2^63, negative as signed long
        long wellAboveBoundary = 0xFFFFFFFFFFFFFFF0L;

        CollectionsManifest manifest = CollectionsManifest.EMPTY_MANIFEST;
        manifest = manifest.withScope(justBelowSignedMax, 1, "scope1");
        assertEquals(justBelowSignedMax, manifest.getUid());

        manifest = manifest.withManifestId(atUnsignedBoundary);
        assertEquals(atUnsignedBoundary, manifest.getUid());

        manifest = manifest.withManifestId(wellAboveBoundary);
        assertEquals(wellAboveBoundary, manifest.getUid());
    }
}
