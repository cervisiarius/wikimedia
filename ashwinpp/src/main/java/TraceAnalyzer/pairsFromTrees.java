package org.wikimedia.ashwinpp.TraceAnalyzer;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import org.junit.Test;


public class pairsFromTrees extends EvalFunc<DataBag> {
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0){
            return null;
        }
        try{
            String treestr = (String)input.get(0);
            TreeNode root;
            Gson gson = new Gson();
            root = gson.fromJson(treestr, TreeNode.class);
            return root.generatePairsBag();
        }catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
    public Schema outputSchema(Schema input) {
        try{
            List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
            fields.add(new Schema.FieldSchema("parent", DataType.CHARARRAY));
            fields.add(new Schema.FieldSchema("current", DataType.CHARARRAY));

            Schema tupleSchema = new Schema(fields);

            Schema.FieldSchema tupleFs;
            tupleFs = new Schema.FieldSchema("pair", tupleSchema, DataType.TUPLE);
            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema("bag_of_pairs",bagSchema, DataType.BAG);
            return new Schema(bagFs);
        }catch (Exception e){
            return null;
        }
    }
    
}



