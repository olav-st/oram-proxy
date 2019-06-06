package eoram.cloudexp.schemes.primitives;

import eoram.cloudexp.utils.Errors;

import java.security.SecureRandom;
import java.util.BitSet;

public class TreeBasedUtils {
	/*
	 * Given 'n' that has 'w' bits, output 'res' whose most significant 'i' bits are identical to 'n' but the rest are 0.
	 */
	public static int iBitsPrefix(int n, int w, int i) {
		return (~((1<<(w-i)) - 1)) & n;
	}

	/*
	 * keys, from 1 to D, are sorted in ascending order
	 */
	static void refineKeys(int[] keys, int d, int z) {
		int j = 0, k = 1;
		for (int i = 1; i <= d && j < keys.length; i++) {
			while (j < keys.length && keys[j] == i) {
				keys[j] = (i-1)*z + k;
				j++; k++;
			}
			if (k <= z)
				k = 1;
			else
				k -= z;
		}
	}


//		private static <T> void swap(T[] arr, int i, int j) {
//			T temp = arr[i];
//			arr[i] = arr[j];
//			arr[j] = temp;
//		}

	public static void writePositionMap(int C, BitSet[] map, Tree st, int index, int val) {
		int base = (index % C) * st.D;
		writeBitSet(map[index/C], base, val, st.D);
	}

	public static BitSet writeBitSet(BitSet map, int base, int val, int d) {
		for (int i = 0; i < d; i++) {
			if (((val>>i) & 1) == 1)
				map.set(base + i);
			else
				map.clear(base + i);
		}

		return map;
	}

	static BitSet writeBitSet(BitSet map, int base, BitSet val, int d) {
		for (int i = 0; i < d; i++) {
			if (val.get(i))
				map.set(base + i);
			else
				map.clear(base + i);
		}

		return map;
	}

	public static int readPositionMap(int C, BitSet[] map, Tree st, int index) {
		int base = fastMod(index, C) * st.D;
		int mapIdx = fastDivide(index, C);
		if(mapIdx >= map.length) { Errors.error("Coding FAIL!"); }
		return readBitSet(map[mapIdx], base, st.D);
	}

	public static int readBitSet(BitSet map, int base, int d) {
		int ret = 0;
		for (int i = 0; i < d; i++) {
			if (map.get(base + i) == true)
				ret ^= (1<<i);
		}
		return ret;
	}

	/*
	 * n has to be a power of 2.
	 * Return the number of bits to denote n, including the leading 1.
	 */
	static int bitLength(int n) {
		if (n == 0)
			return 1;

		int res = 0;
		do {
			n = n >> 1;
			res++;
		} while (n > 0);
		return res;
	}

	static int fastMod(int a, int b) {
		// b is a power of 2
		int shifts = (int) (Math.log(b)/Math.log(2));
		return  a & (1<<shifts) - 1;
	}

	static int fastDivide(int a, int b) {
		// b is a power of 2
		int shifts = (int) (Math.log(b)/Math.log(2));
		return  a >> shifts;
	}

	public static byte[] genPRBits(SecureRandom rnd, int len) {
		byte[] b = new byte[len];
		rnd.nextBytes(b);
		return b;
	}
}