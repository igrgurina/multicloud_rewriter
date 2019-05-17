package cloud.sec.core.tools;

import cloud.sec.core.adapter.jdbc.MultiCloudField;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

/**
 * Utilities for creating and composing field sets.
 *
 * @see cloud.sec.core.adapter.jdbc.MultiCloudField
 */
public class MultiCloudFieldSets {
    private MultiCloudFieldSets() {
    }

    /**
     * Creates a field set with a given array of fields.
     */
    public static MultiCloudFieldSet ofList(MultiCloudField... fields) {
        return new ListFieldSet(ImmutableList.copyOf(fields));
    }

    /**
     * Creates a field set with a given collection of fields.
     */
    public static MultiCloudFieldSet ofList(Iterable<? extends MultiCloudField> fields) {
        return new ListFieldSet(ImmutableList.copyOf(fields));
    }

    /**
     * Field set that consists of a list of fields.
     */
    private static class ListFieldSet implements MultiCloudFieldSet {
        private final ImmutableList<MultiCloudField> fields;

        ListFieldSet(ImmutableList<MultiCloudField> fields) {
            this.fields = fields;
        }

        @Override
        public int hashCode() {
            return fields.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || obj instanceof ListFieldSet
                    && fields.equals(((ListFieldSet) obj).fields);
        }

        public Iterator<MultiCloudField> iterator() {
            return fields.iterator();
        }
    }
}
