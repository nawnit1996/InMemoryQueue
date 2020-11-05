package io.mappedbus.sample.bytearray;
import io.mappedbus.CircularMappaedBusWriter;
import io.mappedbus.CircularMappaedBusWriter2;
import io.mappedbus.MappedBusWriter;

import java.util.Arrays;

public class ByteArrayWriter {

	public static void main(String[] args) {
		ByteArrayWriter writer = new ByteArrayWriter();
		writer.run(1);
	}

	public void run(int source) {
		try {
			CircularMappaedBusWriter2 writer = new CircularMappaedBusWriter2("./test5", 508l);
//			MappedBusWriter writer = new MappedBusWriter("./test6", 100l, 32);

			writer.open();
			
			byte[] buffer ;

			while(true) {
			for (int i = 1; i < 100; i++) {
				buffer= new byte[i];
				Arrays.fill(buffer, (byte)i);
				boolean success = writer.write(buffer, 0, buffer.length);
				if(!success) {
					i--;
				//System.out.println("Save failed trying again for "+(i+1));
				}
				else {
				 System.out.println("saved "+i);
				}
				
				//Thread.sleep(100);
			}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}