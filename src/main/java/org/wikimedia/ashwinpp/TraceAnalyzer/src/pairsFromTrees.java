package TraceAnalyzer;
import java.io.IOException; 
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataBag;

public class triplesFromTrees extends EvalFunc<DataBag> {
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String treestr = (String)input.get(1);
            TreeNode root;
            static Gson gson = new Gson();
            root = gson.fromJson(json_string, TreeNode.class);
            return root.generateTriples();
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
}



