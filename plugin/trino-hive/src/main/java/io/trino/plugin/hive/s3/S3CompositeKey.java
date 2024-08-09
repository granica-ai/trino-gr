package io.trino.plugin.hive.s3;
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.airlift.log.Logger;

public final class S3CompositeKey {
    private static final Logger LOG = Logger.get(S3CompositeKey.class);

    /**
     * Separator used to create composite key of the form objectName#objectETag
     */
    public static final String COMPOSITE_KEY_SEPARATOR = "#";

    // Private constructor to prevent instantiation
    private S3CompositeKey() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Split the input string at the last occurrence of COMPOSITE_KEY_SEPARATOR and
     * return the parts. This method can
     * be used to split composite key of the form "objectName#objectETag" into
     * objectName and objectETag
     *
     * @param input string to be split and message msg to include for debug logs
     * @return Array of strings of length either 1 or 2
     */
    public static String[] splitCompositeKey(String input, String msg) {
        int lastHashIndex = input.lastIndexOf(COMPOSITE_KEY_SEPARATOR);
        String[] result;

        if (lastHashIndex == -1) {
            // No separator found, return the original string
            result = new String[] { input };
        } else {
            // Split the string at the last occurrence of the separator
            String firstPart = input.substring(0, lastHashIndex);
            String secondPart = input.substring(lastHashIndex + 1);

            if (secondPart.isEmpty()) {
                // Second part is empty, return an array with only the first part
                result = new String[] { firstPart };
            } else {
                // Both parts are non-empty, return an array with both parts
                result = new String[] { firstPart, secondPart };
            }
        }
        if (result.length > 1) {
            LOG.debug("composite-key: {} etag found, key split into {} and {}", msg, result[0], result[1]);
        } else {
            LOG.debug("composite-key: {} no etag found, key is {}", msg, result[0]);
        }
        return result;
    }

    // Create composite key - key#Etag
    public static String makeCompositeKey(String key, String etag, String msg) {
        LOG.debug("composite-key: {} creating from {} and {}", msg, key, etag);
        return key + COMPOSITE_KEY_SEPARATOR + etag;
    }

    // Check if the key is a composite key
    public static boolean checkCompositeKey(String key) {
        LOG.debug("composite-key: checking {}", key);
        return key.lastIndexOf(COMPOSITE_KEY_SEPARATOR) >= 0;
    }

    // Get objectName from composite key
    public static String compositeKeyToObjectName(String key) {
        if (key.lastIndexOf(COMPOSITE_KEY_SEPARATOR) < 0) {
            LOG.debug("composite-key: using key as name {}", key);
            return key;
        }
        String[] parts = splitCompositeKey(key, "compositeKeyToObjectName");
        LOG.debug("composite-key: got name {} from {}", parts[0], key);
        return parts[0];
    }
}