package eoram.cloudexp.schemes;

import eoram.cloudexp.evaluation.PerformanceEvaluationLogger;
import eoram.cloudexp.implementation.*;
import eoram.cloudexp.interfaces.*;
import static org.junit.Assert.*;

import java.security.SecureRandom;
import java.util.BitSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Implements tests for the basic PathORAM logic.
 * <p><p>
 * The tests are based on Java code obtained from authors of a follow-up work.
 * <p>
 *
 */
public class TestPathORamBasic 
{
	int N = (int) Math.pow(2,  10); //(int) Math.pow(2, 4);
	int B = 16;
	
	static SecureRandom rnd;
	
	static {
		try {
			rnd = SecureRandom.getInstance("SHA1PRNG");
			rnd.setSeed(new byte[]{1,2,3,4});
		} catch (Exception e) {
			
		}
	}

	int iter = 200;

    @BeforeClass
    public static void oneTimeSetUp() {
        // one-time initialization code   
    	System.out.println("@BeforeClass - oneTimeSetUp");
    	
    	PerformanceEvaluationLogger.getInstance().openCall();
    }
 
    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
    	System.out.println("@AfterClass - oneTimeTearDown");
    	
    	PerformanceEvaluationLogger.getInstance().openDone();
    }

	@Test
	public void testORAMReads() throws Exception 
	{
		
		PathORAMBasic oram = new PathORAMBasic(rnd);
		
		// generate data
		BitSet[] data = new BitSet[N];
		for (int i = 0; i < N; i++) 
		{
			long[] temp = new long[1]; temp[0] = i;
			data[i] = BitSet.valueOf(temp);
		}
		
		ExternalStorageInterface si = new StorageAdapter(new LocalStorage("/tmp/Cloud", true));
		si.connect();
		BitSet[] pm = oram.initialize(si, N, B, data, 0);

		for (int i = 0; i < iter; i++) {
			PathORAMBasic.Tree.Block b = oram.read(0l, pm, i % N);
			assert (b != null) : "read failed when i = " + i;
			assertEquals(b.data, data[i % N]);
		}
	}
	
	@Test
	public void testORAMWrites() throws Exception 
	{
		System.out.println("N = " + N);
		PathORAMBasic oram = new PathORAMBasic(rnd);
		
		BitSet[] data = new BitSet[N];
		for (int i = 0; i < N; i++) 
		{
			long[] temp = new long[1]; temp[0] = i;
			data[i] = BitSet.valueOf(temp);
		}
		
		ExternalStorageInterface si = new StorageAdapter(new LocalStorage("/tmp/Cloud", true));
		si.connect();;
		BitSet[] pm = oram.initialize(si, N, B, data, 0);
//		BitSet[] pm = oram.initialize(N, B);

		for (int i = 0; i < iter; i++) {
			int k = rnd.nextInt(N);
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			data[k] = BitSet.valueOf(temp);

			oram.write(0l, pm, k, data[k]);
			PathORAMBasic.Tree.Block b = oram.read(0l, pm, k);
			assert (b != null) : "read failed when k = " + k;
			assertEquals("break point: i = "+i, b.data, data[k]);
		}
	}
	
	@Test
	public void testORAMReadsAndWrites() throws Exception 
	{
		PathORAMBasic oram = new PathORAMBasic(rnd);
		
		BitSet[] data = new BitSet[N];
		for (int i = 0; i < N; i++) 
		{
			long[] temp = new long[1]; temp[0] = i;
			data[i] = BitSet.valueOf(temp);
		}

		ExternalStorageInterface si = new StorageAdapter(new LocalStorage("/tmp/Cloud", true));
		si.connect();
		BitSet[] pm = oram.initialize(si, N, B, data, 0);
		
		for (int i = 0; i < iter; i++) {
			int k1 = rnd.nextInt(N);
			PathORAMBasic.Tree.Block b = oram.read(0l, pm, k1);
			byte[] temp = new byte[B];
			rnd.nextBytes(temp);
			b.data = BitSet.valueOf(temp);
			

			oram.write(0l, pm, k1, b.data);

			data[k1] = b.data;
			
			int k2 = rnd.nextInt(N);
			b = oram.read(0l, pm, k2);
			assertEquals(b.data, data[k2]);
		}
	}
	
	@Test
	public void testAllOnVariousN() throws Exception 
	{
		for (int i = 8; i < 12; i++) {
			N = (int)Math.pow(2, i);
			testORAMReads();
			testORAMWrites();
			testORAMReadsAndWrites();
		}
	}
}