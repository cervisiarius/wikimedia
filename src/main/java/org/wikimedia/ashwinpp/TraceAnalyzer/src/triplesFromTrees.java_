package TraceAnalyzer;
import java.io.IOException; 
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataBag;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.Gson;

import org.apache.pig.test.util;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import org.junit.Test;


public class triplesFromTrees extends EvalFunc<DataBag> {
    public DataBag exec(Tuple input) throws IOException {
//        PigLogger p = new PigLogger();
 //       p.warn(this, "test", PigWarning.UDF_WARNING_1);
//        System.out.println("aha");
        if (input == null || input.size() == 0){
            return null;
        }
        try{
            String treestr = (String)input.get(0);
            TreeNode root;
            //Gson gson = new Gson();
            //root = gson.fromJson(treestr, TreeNode.class);
            TupleFactory mTupleFactory = TupleFactory.getInstance();
            BagFactory mBagFactory = BagFactory.getInstance();
            DataBag output = mBagFactory.newDefaultBag();
            List<String> tupleList= new LinkedList<String>();
            tupleList.add("");
            tupleList.add("");
            tupleList.add(treestr);
            output.add(mTupleFactory.newTuple(tupleList));
            //return root.generateTriples();
            return output;
        }catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
    public Schema outputSchema(Schema input) {
        try{
            List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
            fields.add(new Schema.FieldSchema("grandparent", DataType.CHARARRAY));
            fields.add(new Schema.FieldSchema("parent", DataType.CHARARRAY));
            fields.add(new Schema.FieldSchema("current", DataType.CHARARRAY));

            Schema tupleSchema = new Schema(fields);

            Schema.FieldSchema tupleFs;
            tupleFs = new Schema.FieldSchema("triplet", tupleSchema, DataType.TUPLE);
            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema("bag_of_triplets",bagSchema, DataType.BAG);
            return new Schema(bagFs);
        }catch (Exception e){
            return null;
        }
    }
    @Test
    public void testStringSubstr() throws IOException {
        Tuple testTuple = Util.buildTuple(null, 0, 2);
        assertNull("null is null", stringSubstr_.exec(testTuple));

        testTuple = Util.buildTuple("", 0, 2);
        assertEquals("empty string", "", stringSubstr_.exec(testTuple));

        testTuple = Util.buildTuple("abcde", 1, 3);
        assertEquals("lowercase string", "bc", stringSubstr_.exec(testTuple));

        testTuple = Util.buildTuple("abc", 0, 15);
        assertEquals("uppercase string", "abc", stringSubstr_.exec(testTuple));
    }

    
}



