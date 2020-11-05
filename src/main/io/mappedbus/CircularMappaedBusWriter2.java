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
 * Class for writing messages to the bus.
 * <p>
 * Messages can either be message based or byte array based.
 * <p>
 * The typical usage is as follows:
 * 
 * <pre>
 * {
 * 	&#64;code
 * // Construct a writer
 * 	MappedBusWriter writer = new MappedBusWriter("/tmp/test", 100000L, 32);
 * 	writer.open();
 * 
 * // A: write an object based message
 * 	PriceUpdate priceUpdate = new PriceUpdate();
 *
 * 	writer.write(priceUpdate);
 * 
 * // B: write a byte array based message
 * 	byte[] buffer = new byte[32];
 *
 * 	writer.write(buffer, 0, buffer.length);
 *
 * // Close the writer
 * 	writer.close();
 * }
 * </pre>
 */
public class CircularMappaedBusWriter2 {

	private MemoryMappedFile mem;

	private final String fileName;

	private final long fileSize;

	//private final int entrySize;

	private final long startLoc = 0l;
	private final long endLoc = 8l;
	private final long flagLoc = 16;
	private final long lastFlipLoc = 20;
	private final long writeUpdate =28;
	private final long readUpdate =32;
	private final long miscSize = 28+8l;

	/**
	 * Constructs a new writer.
	 * 
	 * @param fileName   the name of the memory mapped file
	 * @param fileSize   the maximum size of the file
	 * @param recordSize the maximum size of a record (excluding status flags and
	 *                   meta data)
	 */
	public CircularMappaedBusWriter2(String fileName, long fileSize) {
		this.fileName = fileName;
		this.fileSize = fileSize;
		//this.entrySize = recordSize + Length.RecordHeader;
	}

	/**
	 * Opens the writer.
	 *
	 * @throws IOException if there was an error opening the file
	 */
	public void open() throws IOException {
		try {
			mem = new MemoryMappedFile(fileName, fileSize);
		} catch (Exception e) {
			throw new IOException("Unable to open the file: " + fileName, e);
		}

		mem.compareAndSwapLong(startLoc, 0, miscSize);
		mem.compareAndSwapLong(endLoc, 0, miscSize);
		mem.compareAndSwapLong(readUpdate, 0, 1);
		mem.compareAndSwapLong(writeUpdate, 0, 1);
	}

	/**
	 * Writes a message.
	 *
	 * @param message the message object to write
	 * @return returns true if the message could be written, or otherwise false
	 * @throws EOFException in case the end of the file was reached
	 */
//	public boolean write(MappedBusMessage message) throws EOFException {
//		long commitPos = writeRecord(message);
//		return commit(commitPos);
//	}

//	protected long writeRecord(MappedBusMessage message) throws EOFException {
//		long limit = allocate();
//		long commitPos = limit;
//		limit += Length.StatusFlag;
//		mem.putInt(limit, message.type());
//		limit += Length.Metadata;
//		message.write(mem, limit);
//		return commitPos;
//	}

	/**
	 * Writes a buffer of data.
	 *
	 * @param src    the output buffer
	 * @param offset the offset in the buffer of the first byte to write
	 * @param length the length of the data
	 * @return returns true if the message could be written, or otherwise false
	 * @throws EOFException in case the end of the file was reached
	 */
	public boolean write(byte[] src, int offset, int length) throws EOFException {
		long commitPos = writeRecord(src, offset, length);
		if(commitPos== -1) {
			return false;
		}
		 
				if(commit(commitPos)) {
//					mem.getAndAddLong(endLoc, length+Length.RecordHeader);
					return true;
				}
				return false;
	}

	protected long writeRecord(byte[] src, int offset, int recordSize) throws EOFException {
		long limit = allocate(recordSize);
		if(limit==-1) {
			return -1;
		}
		long commitPos = limit;
		mem.putIntVolatile(commitPos,  StatusFlag.NotSet);		
		limit += Length.StatusFlag;
		mem.putInt(limit, recordSize);
		limit += Length.Metadata;
		mem.setBytes(limit, src, offset, recordSize);
		return commitPos;
	}

	
	synchronized private long allocate(final int recordSize) throws EOFException {
				
		long endP = mem.getLongVolatile(endLoc);
		long startP = mem.getLongVolatile(startLoc);
		int flag =mem.getInt(flagLoc);
		
		
		if (endP + recordSize+Length.RecordHeader >= fileSize) {
			if(mem.getInt(readUpdate)==0) {
				return -1;
			}
			if(flag==0) { //reader is behind or may be at equal
				int rup=mem.getInt(readUpdate);
				System.out.println(String.format("%s %s %s %s ",rup,startP,endP,flag));
				
				 startP = mem.getLong(startLoc);
				 flag =mem.getInt(flagLoc);
				if(endP==startP) { //Reader consumed all from current batch
//					mem.putIntVolatile(writeUpdate, 0);
					mem.putLongVolatile(lastFlipLoc, endP);
					mem.compareAndSwapInt(flagLoc, 0, 1);
					mem.compareAndSwapLong(endLoc, endP, miscSize);
					mem.putIntVolatile(readUpdate, 0);
					System.out.println(String.format("%s %s %s ",startP,endP,flag));
					endP=miscSize;
				}else if(miscSize+recordSize<startP) { //reader is reading current batch
//					mem.putIntVolatile(writeUpdate, 0);
					mem.putLongVolatile(lastFlipLoc, endP);
					mem.compareAndSwapInt(flagLoc, 0, 1);
					mem.compareAndSwapLong(endLoc, endP, miscSize);
					mem.putIntVolatile(readUpdate, 0);
					System.out.println(String.format("%s %s %s ",startP,endP,flag));
					endP=miscSize;
				}else { //Not enough data in beginning
					return -1l;
				}
			}
			return -1l;
		}	
		if(flag==1) { //Second batch
			if(endP + recordSize+Length.RecordHeader >= startP) { //Don't override unread record
				return -1;
			}
		}
		mem.putIntVolatile(endP, 0);
		return mem.getAndAddLong(endLoc, recordSize+Length.RecordHeader);

		
	  
		//Is end behind start ?
//		if(mem.getInt(flagLoc)==1) {
//			long startP = mem.getLongVolatile(startLoc);
//			System.out.println("start is lagging , start= "+startP +"  end ="+endP);
//			if (endP+recordSize>= startP) {
//				//can't write, retry again
//				//System.out.println("Message was not read , queue is full. can'r write");
//				return -1;
//			}
//		}
//
//		//long limit = mem.getAndAddLong(endLoc, recordSize);
//		
//		return endP;
	}

	protected boolean commit(long commitPos) {
		return mem.compareAndSwapInt(commitPos, StatusFlag.NotSet, StatusFlag.Commit);
	}

	/**
	 * Closes the writer.
	 *
	 * @throws IOException if there was an error closing the file
	 */
	public void close() throws IOException {
		try {
			mem.unmap();
		} catch (Exception e) {
			throw new IOException("Unable to close the file", e);
		}
	}
}