package io.mappedbus.sample.bytearray;
import io.mappedbus.CircularMappedBusReader;
import io.mappedbus.CircularMappedBusReader2;
import io.mappedbus.MappedBusReader;

import java.util.Arrays;

public class ByteArrayReader {

	public static void main(String[] args) {
		ByteArrayReader reader = new ByteArrayReader();
		reader.run();	
	}

	public void run() {
		try {
			CircularMappedBusReader2 reader = new CircularMappedBusReader2("./test5", 508l);
//			MappedBusReader reader = 	new MappedBusReader("./test6", 100l, 32);

			reader.open();

			byte[] buffer;

			while (true) {
				if (reader.next()) {
					buffer = reader.readBuffer( 0);
//					if(length==-1) {
//						System.out.println("Read failed retrying again");
//						continue;
//					}
					System.out.println("Read: length = " + buffer.length );
				}
//				Thread.sleep(500);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}