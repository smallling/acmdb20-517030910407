package simpledb;

import java.time.chrono.MinguoChronology;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupAns;
    private HashMap<Field, Integer> groupCnt;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupAns = new HashMap<>();
        groupCnt = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = null;
        if(gbfield != Aggregator.NO_GROUPING) {
            groupField = tup.getField(gbfield);
        }
        if(!groupAns.containsKey(groupField)) {
            if(what == Op.MAX) {
                groupAns.put(groupField, Integer.MIN_VALUE);
            }
            if(what == Op.MIN) {
                groupAns.put(groupField, Integer.MAX_VALUE);
            }
            if(what == Op.COUNT || what == Op.SUM) {
                groupAns.put(groupField, 0);
            }
            if(what == Op.AVG) {
                groupCnt.put(groupField, 0);
                groupAns.put(groupField, 0);
            }
        }
        int newVal = ((IntField) tup.getField(afield)).getValue();
        int val = groupAns.get(groupField);
        if(what == Op.MAX) {
            val = Integer.max(newVal, val);
        }
        if(what == Op.MIN) {
            val = Integer.min(newVal, val);
        }
        if(what == Op.COUNT) {
            val++;
        }
        if(what == Op.SUM) {
            val += newVal;
        }
        if(what == Op.AVG) {
            val += newVal;
            int tmp = groupCnt.get(groupField);
            groupCnt.put(groupField, tmp + 1);
        }
        groupAns.put(groupField, val);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        String groupName[];
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
            if(what == Op.AVG) {
                val /= groupCnt.get(key);
            }
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

