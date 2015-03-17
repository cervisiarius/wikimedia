package TraceAnalyzer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import java.util.LinkedList;
import java.util.Queue;
import java.util.List;



public class TreeNode {
    String dt, uri_path, uri_query, referer, content_type, http_status;
    boolean parent_ambiguous, bad_tree;
    TreeNode[] children;

    class Triplet{
        TreeNode grandparent, parent, current;
        Triplet(TreeNode gp,TreeNode p,TreeNode c){
            grandparent = gp;
            parent = p;
            c = c;
        }
    }
    public DataBag generateSingletons(){
        Queue<TreeNode> pageQueue = new LinkedList<TreeNode>();
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        BagFactory mBagFactory = BagFactory.getInstance();
        DataBag output = mBagFactory.newDefaultBag();
        TreeNode root = this;
        pageQueue.add(root); 
        while(!pageQueue.isEmpty()){ 
            TreeNode node = pageQueue.remove();
            output.add(mTupleFactory.newTuple(node.uri_path));
            if (children!=null){
                for(int i=0; i<children.length;i++){
                    pageQueue.add(children[i]);
                }
            }
        }
        return output;
    }
    public DataBag generatePairs(){
        Queue<Triplet> pageQueue = new LinkedList<Triplet>();
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        BagFactory mBagFactory = BagFactory.getInstance();
        DataBag output = mBagFactory.newDefaultBag();

        Triplet root = new Triplet(null, null, this);
        pageQueue.add(root);
        while(!pageQueue.isEmpty()){
            Triplet node = pageQueue.remove();
            if(node.parent !=null){
                List<String> tupleList= new LinkedList<String>();
                tupleList.add(node.parent.uri_path);
                tupleList.add(node.current.uri_path);
                output.add(mTupleFactory.newTuple(tupleList));
            }
            if (children!=null){
                for(int i=0; i<children.length;i++){
                    pageQueue.add(new Triplet(node.parent, node.current, children[i]));
                }
            }
        }
        return output;
    }

    public DataBag generateTriples(){
        Queue<Triplet> pageQueue = new LinkedList<Triplet>();
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        BagFactory mBagFactory = BagFactory.getInstance();
        DataBag output = mBagFactory.newDefaultBag();

        Triplet root = new Triplet(null, null, this);
        pageQueue.add(root);
        while(!pageQueue.isEmpty()){
            Triplet node = pageQueue.remove();
            if(node.grandparent !=null){
                assert(node.parent !=null);
                List<String> tupleList= new LinkedList<String>();
                tupleList.add(node.grandparent.uri_path);
                tupleList.add(node.parent.uri_path);
                tupleList.add(node.current.uri_path);
                output.add(mTupleFactory.newTuple(tupleList));
            }
            if (children!=null){
                for(int i=0; i<children.length;i++){
                    pageQueue.add(new Triplet(node.parent, node.current, children[i]));
                }
            }
        }
        return output;
    }

}
