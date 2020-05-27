package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupAns;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupAns = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbfield == Aggregator.NO_GROUPING) {
            return;
        }
        Field groupField = tup.getField(gbfield);
        if(!groupAns.containsKey(groupField)) {
            groupAns.put(groupField, 1);
            return;
        }
        int val = groupAns.get(groupField) + 1;
        groupAns.put(groupField, val);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        String groupName[] = null;
        Type groupType[] ;
        if(gbfield == Aggregator.NO_GROUPING) {
            groupName = new String[1];
            groupType = new Type[1];
            groupName[0] = "aggregateVal";
            groupType[0] = Type.INT_TYPE;
        }
        else {
            groupName = new String[2];
            groupType = new Type[2];
            groupName[0] = "groupVal";
            groupType[0] = gbfieldtype;
            groupName[1] = "aggregateVal";
            groupType[1] = Type.INT_TYPE;
        }
        TupleDesc td = new TupleDesc(groupType, groupName);

        ArrayList<Tuple> tuples = new ArrayList<>();
        for(Field key : groupAns.keySet()) {
            int val = groupAns.get(key);
            Tuple curTuple = new Tuple(td);
            if(gbfield == Aggregator.NO_GROUPING) {
                curTuple.setField(0, new IntField(val));
            }
            else {
                curTuple.setField(0, key);
                curTuple.setField(1, new IntField(val));
            }
            tuples.add(curTuple);
        }
        return new TupleIterator(td, tuples);
    }

}
