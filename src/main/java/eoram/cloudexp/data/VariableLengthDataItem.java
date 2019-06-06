package eoram.cloudexp.data;

import eoram.cloudexp.artifacts.ClientParameters;

import java.io.*;
import java.util.Arrays;

public class VariableLengthDataItem extends DataItem
{
	protected int length = 0;
	protected byte[] data = null;

	ClientParameters clientParams = ClientParameters.getInstance();

	public VariableLengthDataItem(byte[] d) throws IOException
	{
		data = d;
		int i = data.length -1;
		while(i >= 0 && data[i] == 0)
		{
			i--;
		}
		length = i;
	}

	public VariableLengthDataItem(byte[] d, int offset, int l) throws IOException
	{
		if(l > getMaxDataLength())
		{
			throw new IllegalArgumentException("Data longer than max length");
		}
		length = l;
		data = Arrays.copyOfRange(d, offset, offset + clientParams.contentByteSize);
		data[length] = 1;
		for(int i = length + 1; i < clientParams.contentByteSize; i++)
		{
			data[i] = 0;
		}
	}

	@Override
	public synchronized byte[] getData() { return data; }

	public synchronized int getStart() { return 0; }

	public synchronized int getLength() { return length; }

	public static int getMaxDataLength()
	{
		ClientParameters clientParams = ClientParameters.getInstance();
		return clientParams.contentByteSize - 1;
	}
}
