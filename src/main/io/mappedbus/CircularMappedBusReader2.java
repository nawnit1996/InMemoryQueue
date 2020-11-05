/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */
package io.mappedbus;

import io.mappedbus.MappedBusConstants.StatusFlag;
import io.mappedbus.MappedBusConstants.Length;
import io.mappedbus.MappedBusConstants.Structure;

import java.io.EOFException;
import java.io.IOException;

/**
 * Class for reading messages from the bus.
 * <p>
 * Messages can either be message based or byte array based.
 * <p>
 * The typical usage is as follows:
 * <pre>
 * {@code
 * // Construct a reader
 * MappedBusReader reader = new MappedBusReader("/tmp/test", 100000L, 32);
 * reader.open();
 * 
 * // A: read messages as objects
 * while (true) {
 *    if (reader.next()) {
 *       int type = reader.readType();
 *       if (type == 0) {
 *          reader.readMessage(priceUpdate)
 *       }
 *    }
 * }
 *
 * // B: read messages as byte arrays
 * while (true) {
 *    if (reader.next()) {
 *       int length = reader.read(buffer, 0);
 *    }
 * }
 *
 * // Close the reader
 * reader.close();
 * }
 * </pre>
 */
public class CircularMappedBusReader2 {

	protected static final long MAX_TIMEOUT_COUNT = 100;

	private final String fileName;

	private final long fileSize;

	//private final int recordSize;

	private MemoryMappedFile mem;

	private long limit = Structure.Data;

	private long prevLimit = 0;

	private long initialLimit;

	private int maxTimeout = 2000;

	protected long timerStart;

	protected long timeoutCounter;

	private boolean typeRead;
	
	
	private final long startLoc = 0l;
	private final long endLoc = 8l;
	private final long flagLoc = 16;
	private final long lastFlipLoc = 20;
	private final long writeUpdate =28;
	private final long readUpdate =32;
	private final long miscSize = 28+8;

	
	private  long readPointer;
	private long readPointerwithoutRead;
	/**
	 * Constructs a new reader.
	 *
	 * @param fileName the name of the memory mapped file
	 * @param fileSize the maximum size of the file
	 * @param recordSize the maximum size of a record (excluding status flags and meta data)
	 */
	public CircularMappedBusReader2(String fileName, long fileSize) {
		this.fileName = fileName;
		this.fileSize = fileSize;
		//this.recordSize = recordSize;
	}

	/**
	 * Opens the reader.
	 *
	 * @throws IOException if there was a problem opening the file
	 */
	public void open() throws IOException {
		try {
			mem = new MemoryMappedFile(fileName, fileSize);
		} catch(Exception e) {
			throw new IOException("Unable to open the file: " + fileName, e);
		}
		mem.compareAndSwapLong(startLoc, 0, miscSize);
		mem.compareAndSwapLong(endLoc, 0, miscSize);
		mem.compareAndSwapLong(readUpdate, 0, 1);
		mem.compareAndSwapLong(writeUpdate, 0, 1);

		initialLimit = mem.getLongVolatile(Structure.Limit);
	}

	/**
	 * Sets the time for a reader to wait for a record to be committed.
	 *
	 * When the timeout occurs the reader will mark the record as "rolled back" and
	 * the record is ignored.
	 * 
	 * @param timeout the timeout in milliseconds
	 */
	public void setTimeout(int timeout) {
		this.maxTimeout = timeout;
	}

	/**
	 * Steps forward to the next record if there's one available.
	 * 
	 * The method has a timeout for how long it will wait for the commit field to be set. When the timeout is
	 * reached it will set the roll back field and skip over the record. 
	 *
	 * @return true, if there's a new record available, otherwise false
	 * @throws EOFException in case the end of the file was reached
	 */
	public boolean next() throws EOFException {
//		if(mem.getLongVolatile(writeUpdate)==0) {
//			return false;
//		}
		long startP = mem.getLongVolatile(startLoc);
		long endP = mem.getLongVolatile(endLoc);
		long lastFlip = mem.getLongVolatile(lastFlipLoc);
		int flag = mem.getInt(flagLoc);
		if (lastFlip== startP) {
			if(flag==1) {
				if(endP ==miscSize || endP ==startP) { //just flipped, hang on dude.
					return false;
				}else { //Wrote at least one after flipping.
					System.out.println(String.format("%s %s %s %s ",startP,endP,lastFlip,flag));
//					mem.putIntVolatile(readUpdate, 0);
					mem.compareAndSwapInt(flagLoc, 1, 0); 
					mem.compareAndSwapLong(startLoc, startP, miscSize);
					mem.putIntVolatile(readUpdate, 1);
					startP=miscSize;
				}
				
			}
			return false; //No data,writer is slow or out of data, Duh!.
		}
		else {
			if(flag==0) {
				if(startP>=endP) { //Watch out!!, There is hell beyond writer
				      return false;
				     }
			}else {
				if(endP>=startP) { //C'mon man, Writer already finished on lap.
					return false;
				 }
				}
			}
			// throw new EOFException("End of file was reached");
			//System.out.println("End of file has reached, switiching end pointer");
//			long end= mem.getLongVolatile(endLoc);
//			if( end == lastFlip || end== miscSize) {
//				return false;
//			}
//			mem.compareAndSwapLong(startLoc, startP, miscSize);
//			startP=miscSize;
//			boolean updated = false;
//			while (!updated) {
//				updated = mem.compareAndSwapInt(flagLoc, 0, 1);
//				if (!updated) {
//					updated = mem.compareAndSwapInt(flagLoc, 1, 0);
//				}
//			}
//
//		}
//		
//		//Is end ahead start ?
//		long endP = mem.getLongVolatile(endLoc);
//		System.out.println(" start= "+startP +"  end ="+endP);
//				if(mem.getInt(flagLoc)==0) {
//					
//					if (startP == endP) {
//						//can't write, retry again
//						System.out.println("No new message available , queue is empty. can'r read");
//						return false;
//					}
//				}
//		if (limit >= fileSize) {
//			throw new EOFException("End of file was reached");
//		}
//		if (prevLimit != 0 && limit - prevLimit < Length.RecordHeader + recordSize) {
//			limit = prevLimit + Length.RecordHeader + recordSize;
//		}
//		if (mem.getLongVolatile(Structure.Limit) <= limit) {
//			return false;
//		}
		int statusFlag = mem.getIntVolatile(startP);
		
		if (statusFlag == StatusFlag.Rollback) {
//			limit += Length.RecordHeader + recordSize;
//			prevLimit = 0;
//			timeoutCounter = 0;
//			timerStart = 0;
//			return false;
		}
		if (statusFlag == StatusFlag.Commit) {
			timeoutCounter = 0;
			timerStart = 0;
			prevLimit = limit;
			readPointer= startP;
			readPointerwithoutRead= startP;
			return true;
		}
		timeoutCounter++;
//		if (timeoutCounter >= MAX_TIMEOUT_COUNT) {
//			if (timerStart == 0) {
//				timerStart = System.currentTimeMillis();
//			} else {
//				if (System.currentTimeMillis() - timerStart >= maxTimeout) {
//					if (!mem.compareAndSwapInt(limit, StatusFlag.NotSet, StatusFlag.Rollback)) {
//						// there are two cases this can happen
//						// 1) a slow writer eventually set the status flag to commit
//						// 2) another reader set the status flag to rollback right before this reader was going to
//						// in both cases return false, and the value of the status flag will be used in the next call to this method
//						return false;
//					}
//					limit += Length.RecordHeader + recordSize;
//					prevLimit = 0;
//					timeoutCounter = 0;
//					timerStart = 0;
//					return false;
//				}
//			}
//		}
		return false;
	}

	/**
	 * Reads the message type.
	 *
	 * @return the message type
	 */
	public int readType() {
		typeRead = true;
		readPointer += Length.StatusFlag;
		int type = mem.getInt(readPointer);
		readPointer += Length.Metadata;
		return type;
	}

	/**
	 * Reads the next message.
	 *
	 * @param message the message object to populate
	 * @return the message object
	 */
//	public MappedBusMessage readMessage(MappedBusMessage message) {
//		if (!typeRead) {
//			readType();
//		}
//		typeRead = false;
//		message.read(mem, readPointer);
//		readPointer += recordSize;
//		return message;
//	}

	/**
	 * Reads the next buffer of data.
	 * 
	 * @param dst the input buffer
	 * @param offset the offset in the buffer of the first byte to read data into
	 * @return the length of the record that was read
	 */
	public byte[] readBuffer( int offset) {
		readPointer += Length.StatusFlag;
		int length = mem.getInt(readPointer);
		readPointer += Length.Metadata;
		byte[] data= new byte[length];

		mem.getBytes(readPointer, data, offset, length);
		readPointer += length;
		boolean commit =mem.compareAndSwapLong(startLoc, readPointerwithoutRead, readPointer);
		if(!commit) {
			System.out.println("Commit failed changing, will try reading it again.");
			return null;
		}
		return data;
	}

	/**
	 * Indicates whether all records available when the reader was created have been read.
	 *
	 * @return true, if all records available from the start was read, otherwise false
	 */
	public boolean hasRecovered() {
		return limit >= initialLimit;
	}

	/**
	 * Closes the reader.
	 *
	 * @throws IOException if there was an error closing the file
	 */
	public void close() throws IOException {
		try {
			mem.unmap();
		} catch(Exception e) {
			throw new IOException("Unable to close the file", e);
		}
	}
}