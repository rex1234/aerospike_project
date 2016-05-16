package cz.muni.fi.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;

/**
 * Created by Rex on 7.5.2016.
 */
public class Main {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("0.0.0.0", 3000);

//        deleteAll(client, "Cars");
//        deleteAll(client, "Branches");

//        initializeBranches(client);
//
//        addCarToBranch(client, "Brno Turany", "Mercedes", "GL");
//        addCarToBranch(client, "Brno Turany", "Audi", "A3");
//        addCarToBranch(client, "Brno Turany", "Mercedes", "C3");
//        addCarToBranch(client, "Praha Zizkov", "Skoda", "Fabia");
//        addCarToBranch(client, "Praha Zizkov", "Skoda", "Felicia");
//        addCarToBranch(client, "Praha Zizkov", "Audi", "A5");
//        addCarToBranch(client, "Praha Zizkov", "Fiat", "Multipla");
//
//
//
//        System.out.println("All branches:");
//        client.scanAll(null, "test", "Branches", new ScanCallback() {
//            public void scanCallback(Key key, Record record) throws AerospikeException {
//                System.out.println(record.bins);
//            }
//        });
//
//
//
//        System.out.println("Cars sold in Brno Turany:");
//
//        client.createIndex(null, "test", "Cars", "branch_index", "branch", IndexType.STRING).waitTillComplete();
//
//        Statement stmt = new Statement();
//        stmt.setNamespace("test");
//        stmt.setSetName("Cars");
//        stmt.setFilters(Filter.equal("branch", "Brno Turany"));
//
//        RecordSet recordSet = client.query(null, stmt);
//        while (recordSet.next()) {
//            Record record = recordSet.getRecord();
//            System.out.println("rec som zabil " + record.bins);
//        }
//
//        //branch with max cars
//        System.out.println("Map reduce - most cars sold in a single branch");
//        stmt = new Statement();
//        stmt.setNamespace("test");
//        stmt.setSetName("Branches");
//
//        LuaConfig.SourceDirectory = "udf";
//        client.register(null, "udf/carCount.lua", "carCount.lua", Language.LUA);
//
//        ResultSet resultSet = client.queryAggregate(null, stmt, "carCount", "findMax");
//        while (resultSet.next()) {
//            System.out.println(resultSet.getObject());
//        }

        client.close();
    }

    private static void addCarToBranch(AerospikeClient client, String branch, String carBrand, String carType) {
        try {
            Key key = new Key("test", "Cars", branch + ":" + getAndIncreaseCarCount(client, branch));
            Bin bin = new Bin("brand", carBrand);
            Bin bin1 = new Bin("type", carType);
            Bin bin2 = new Bin("branch", branch);
            client.put(null, key, bin, bin1, bin2);
        } catch (BranchNotFoundException e) {
            System.err.println("Cannot add car to the branch");
            e.printStackTrace();
        }

    }

    private static int getAndIncreaseCarCount(AerospikeClient client, String branch) throws BranchNotFoundException {
        Key branchKey = keyForBranch(branch);

        if(client.exists(null, branchKey)) {
            Record branchRecord = client.get(null, branchKey);
            int carCount = branchRecord.getInt("carCount");
            Bin bin = new Bin("carCount", ++carCount);
            client.put(null, branchKey, bin);
            return carCount;
        } else {
            throw new BranchNotFoundException("Branch " + branch + " not in the database");
        }
    }

    private static void initializeBranches(AerospikeClient client) {
        Key key = keyForBranch("Brno Turany");
        Key key2 = keyForBranch("Praha Zizkov");

        WritePolicy wPolicy = new WritePolicy();
        //CREATE_ONLY police throws an exception when inserting the same record again
        wPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

        if(!client.exists(null, key)) {
            Bin bin1 = new Bin("director", "Jan Mjartan");
            Bin bin2 = new Bin("janitor", "Jan Kardas");
            Bin bin3 = new Bin("address", "Ondrejova 75, 61200 Brno, Czech Republic");
            Bin bin4 = new Bin("carCount", 0);

            client.put(wPolicy, key, bin1, bin2, bin3, bin4);
        }
        if(!client.exists(null, key2)) {
            Bin bin1 = new Bin("director", "Michal Humaj");
            Bin bin2 = new Bin("janitor", "Patrik Vrbovsky");
            Bin bin3 = new Bin("address", "Kacerov 13, 41036 Praha, Czech Republic");
            Bin bin4 = new Bin("carCount", 0);

            client.put(wPolicy, key2, bin1, bin2, bin3, bin4);
        }
    }

    private static void deleteAll(final AerospikeClient client, String set) {
        client.scanAll(null, "test", set, new ScanCallback() {
            public void scanCallback(Key key, Record record) throws AerospikeException {
                client.delete(null, key);
            }
        });
    }

    private static Key keyForBranch(String branch) {
        return new Key("test", "Branches", branch);
    }
}
