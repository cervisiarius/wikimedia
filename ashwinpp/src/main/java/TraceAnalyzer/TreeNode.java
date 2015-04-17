package org.wikimedia.ashwinpp.TraceAnalyzer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import java.util.LinkedList;
import org.apache.commons.lang.StringUtils;
import java.util.Queue;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.Gson;
import org.apache.commons.lang.ArrayUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class TreeNode {
    String dt, uri_path, uri_query, referer, content_type, http_status;
    boolean parent_ambiguous, bad_tree;
    TreeNode[] children;

    class Triplet{
        TreeNode grandparent, parent, current;
        Triplet(TreeNode gp,TreeNode p,TreeNode c){
            grandparent = gp;
            parent = p;
            current = c;
        }
    }
    private TreeNode[] mergeChildren(TreeNode[] children){
        Map childrenMap = new HashMap<String, TreeNode>();
        for(int i=0; i<children.length;i++){
            String uri = children[i].uri_path;
            if(childrenMap.containsKey(uri)){
                TreeNode c1 = (TreeNode)childrenMap.get(uri);
                c1.children =(TreeNode[]) ArrayUtils.addAll(c1.children, children[i].children);
            }
            else{
                childrenMap.put(uri, children[i]);
            }
        }
        return (TreeNode[])childrenMap.values().toArray(new TreeNode[childrenMap.size()]);
    }
            
    private List< List< String>> generateSingletons(){
        Queue<TreeNode> pageQueue = new LinkedList<TreeNode>();
        TreeNode root = this;
        pageQueue.add(root); 
        List<List<String>> output = new LinkedList<List<String>>();
        while(!pageQueue.isEmpty()){ 
            TreeNode node = pageQueue.remove();
            List<String> tuple = new ArrayList<String>(1);
            tuple.add(0, node.uri_path);
            output.add(tuple);
            if (node.children!=null){
                node.children = mergeChildren(node.children);
                for(int i=0; i<node.children.length;i++){
                    pageQueue.add(node.children[i]);
                }
            }
        }
        return output;
    }
    private List< List< String>> generatePairs(){
        Queue<Triplet> pageQueue = new LinkedList<Triplet>();
        Triplet root = new Triplet(null, null, this);
        pageQueue.add(root);
        List<List<String>> output = new LinkedList<List<String>>();
        while(!pageQueue.isEmpty()){
            Triplet node = pageQueue.remove();
            if(node.parent !=null){
                List<String> tupleList= new LinkedList<String>();
                tupleList.add(node.parent.uri_path);
                tupleList.add(node.current.uri_path);
                output.add(tupleList);
            }
            TreeNode current = node.current;
            if (current.children!=null){
                current.children = mergeChildren(current.children);
                for(int i=0; i<current.children.length;i++){
                    pageQueue.add(new Triplet(node.parent, current, current.children[i]));
                }
            }
        }
        return output;
    }
    private List< List< String>> generateTriples(){
        Queue<Triplet> pageQueue = new LinkedList<Triplet>();
        Triplet root = new Triplet(null, null, this);
        pageQueue.add(root);
        List<List<String>> output = new LinkedList<List<String>>();
        while(!pageQueue.isEmpty()){
            Triplet node = pageQueue.remove();
            if(node.grandparent !=null){
                assert(node.parent !=null);
                List<String> tupleList= new LinkedList<String>();
                tupleList.add(node.grandparent.uri_path);
                tupleList.add(node.parent.uri_path);
                tupleList.add(node.current.uri_path);
                output.add(tupleList);
            }
            TreeNode current  = node.current;
            if (current.children!=null){
                current.children = mergeChildren(current.children);
                for(int i=0; i<current.children.length;i++){
                    pageQueue.add(new Triplet(node.parent, current,  current.children[i]));
                }
            }
        }
        return output;
    }
    private String convertToString(List<List<String>> input){
        String output = ""; 
        for (List<String> tupleList: input){
            String tupleString = StringUtils.join(tupleList, '\t');
            output+=(tupleString+'\n');
        }
        return output;
    }
    private DataBag convertToBag(List<List<String>> input){
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        BagFactory mBagFactory = BagFactory.getInstance();
        DataBag output = mBagFactory.newDefaultBag();
        for (List<String> tupleList: input){
            output.add(mTupleFactory.newTuple(tupleList));
        }
        return output;
    }
    public String generateSingletonString(){
        return convertToString(generateSingletons());
    }
    public String generatePairsString(){
        return convertToString(generatePairs());
    }
    public String generateTriplesString(){
        return convertToString(generateTriples());
    }
    
    public DataBag generateSingletonsBag(){
        return convertToBag(generateSingletons());
    }
    public DataBag generatePairsBag(){
        return convertToBag(generatePairs());
    }

    public DataBag generateTriplesBag(){
        return convertToBag(generateTriples());
    }
    public static void main(String[] args) {
        try{
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
 
            String input;
 
            while((input=br.readLine())!=null){
                Gson gson = new Gson();
                TreeNode tn = gson.fromJson(input, TreeNode.class);
                System.out.println(tn.generateSingletonString());

            }
 
        }catch(IOException io){
            io.printStackTrace();
        }   
    }
}
