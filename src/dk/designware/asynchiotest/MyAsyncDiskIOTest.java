package dk.designware.asynchiotest;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyAsyncDiskIOTest {
	
	private static final Random rand = new Random();
	
	private static String fileName;
	private static long no1KBlocks;
	private static int noFaculties;
	private static int facultyOf;
	
	public static void main(String[] args) throws Exception {
		fileName = args[0];
		no1KBlocks = Long.parseLong(args[1]);
		noFaculties = Integer.parseInt(args[2]);
		facultyOf = Integer.parseInt(args[3]);
		String mode = args[4];
		int threads = (args.length > 5)?Integer.parseInt(args[5]):-1;
		
		
		System.out.println("Creating 1024 * " + no1KBlocks + " sized file at " + fileName);
		createFile(new File(fileName), 1024, no1KBlocks);
		System.out.println("Finished creating file " + fileName);

		ExecutorService es = (mode.contains("threaded"))?Executors.newFixedThreadPool(threads):null;
		System.out.println(new Date() + " Starting " + mode + " (" + threads + ")");
		long start = System.currentTimeMillis();
		try {
			if (mode.equals("synch")) {
				synchFacultySum();
			} else if (mode.equals("synch_threaded")) {
				synchThreadedFacultySum(es);
			} else if (mode.equals("asynch_threaded")) { 
				asynchFacultySum(es);
			} else if (mode.equals("asynch_future")) {
				asynchFutureFacultySum();
			}
			System.out.println(new Date() + " Finished " + mode + " (" + threads + ") in " + (System.currentTimeMillis() - start) + " ms");
		} finally {
			if (es != null) es.shutdown();
		}
	}
	
	private static void synchFacultySum() throws Exception {
		RandomAccessFile raf = new RandomAccessFile(fileName, "r");
		try {
			byte[] bytes = new byte[10];
			AtomicLong totalFacultySum = new AtomicLong();
			for (int i = 0; i < noFaculties; i++) {
				raf.seek(Math.abs(rand.nextLong())%(raf.length() - 10));
				int noRead = raf.read(bytes);
				for (int j = 0; j < noRead; j++) {
					faculty((byte)(bytes[j]), totalFacultySum);
				}
			}
			System.out.println("Total faculty sum: " + totalFacultySum);
		} finally {
			raf.close();
		}
	}

	private static void synchThreadedFacultySum(ExecutorService es) throws Exception {
		RandomAccessFile raf = new RandomAccessFile(fileName, "r");
		try {
			byte[] bytes = new byte[10*noFaculties];
			int offset = 0;
			AtomicLong totalFacultySum = new AtomicLong();
			AtomicInteger completedCount = new AtomicInteger();
			for (int i = 0; i < noFaculties; i++) {
				FacultyRunnable fr = new FacultyRunnable(totalFacultySum, completedCount, noFaculties, raf, bytes, offset, 10);
				es.execute(fr);
				offset += 10;
			}
			synchronized(FacultyRunnable.class) {
				FacultyRunnable.class.wait();
			}
			System.out.println("Total faculty sum: " + totalFacultySum.get());

		} finally {
			raf.close();
		}
	}
	
	private static class FacultyRunnable implements Runnable {
		
		public final int finishedOnCompletionCount;
		private final RandomAccessFile raf;
		private final byte[] bytes;
		private final int offset;
		private final int len;
		
		public final AtomicLong totalFacultySum;
		private final AtomicInteger completedCount;

		public FacultyRunnable(AtomicLong totalFacultySum, AtomicInteger completedCount ,int finishedOnCompletionCount, RandomAccessFile raf, byte[] bytes, int offset, int len) {
			this.totalFacultySum = totalFacultySum;
			this.completedCount = completedCount;
			this.finishedOnCompletionCount = finishedOnCompletionCount;
			this.raf = raf;
			this.bytes = bytes;
			this.offset = offset;
			this.len = len;
		}
		
		@Override
		public void run() {
			try {
				raf.seek(Math.abs(rand.nextLong())%(raf.length() - len));
				int noRead = raf.read(bytes, offset, len);
				for (int j = 0; j < noRead; j++) {
					faculty((byte)(bytes[j]), totalFacultySum);
				}
				completedCount.incrementAndGet();
				if (completedCount.intValue() == finishedOnCompletionCount) {
					synchronized(FacultyRunnable.class) {
						FacultyRunnable.class.notify();
					}
				}
			} catch (Throwable t) {
				System.out.println("Failed");
				t.printStackTrace(System.out);
			}
		}

	}
	
	private static void asynchFacultySum(ExecutorService es) throws Exception {
		Path path = FileSystems.getDefault().getPath(fileName);
		Set<StandardOpenOption> opts = new HashSet<StandardOpenOption>();
		opts.add(StandardOpenOption.READ);
		AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, opts, es, new FileAttribute<?>[0]);
		try {
			FacultyCompletionHandler ch = new FacultyCompletionHandler(noFaculties);
			byte[] bytes = new byte[10*noFaculties];
			int offset = 0;
			for (int i = 0; i < noFaculties; i++) {
				ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, 10);
				offset += 10;
				afc.read(buffer, Math.abs(rand.nextLong())%(afc.size() - buffer.remaining()), buffer, ch);
			}
			synchronized(ch) {
				ch.wait();
			}
			System.out.println("Total faculty sum: " + ch.totalFacultySum.get());
		} finally {
			afc.close();
		}
	}
	
	private static class FacultyCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
		
		private final int finishedOnCompletionCount;
		
		public AtomicLong totalFacultySum;
		private AtomicInteger completedCount;

		public FacultyCompletionHandler(int finishedOnCompletionCount) {
			totalFacultySum = new AtomicLong();
			completedCount = new AtomicInteger();
			this.finishedOnCompletionCount = finishedOnCompletionCount;
		}
		
		@Override
		public void completed(Integer noRead, ByteBuffer byteBuffer) {
			byte[] bytes = byteBuffer.array();
			int offset = byteBuffer.arrayOffset();
			for (int i = 0; i < noRead; i++) {
				faculty((byte)(bytes[offset++]), totalFacultySum);
			}
			completedCount.incrementAndGet();
			if (completedCount.intValue() == finishedOnCompletionCount) {
				synchronized(this) {
					this.notify();
				}
			}
		}

		@Override
		public void failed(Throwable t, ByteBuffer byteBuffer) {
			System.out.println("Failed");
			t.printStackTrace(System.out);
		}
		
	}
	
	private static void asynchFutureFacultySum() throws Exception {
		Path path = FileSystems.getDefault().getPath(fileName);
		AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
		try {
			FutureIntegerByteBuffer[] fibb = new FutureIntegerByteBuffer[noFaculties]; 
			byte[] bytes = new byte[10*noFaculties];
			AtomicLong totalFacultySum = new AtomicLong();
			int offset = 0;
			int nextIndexToHandle = 0;
			for (int i = 0; i < noFaculties; i++) {
				ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, 10);
				offset += 10;
				fibb[i] = new FutureIntegerByteBuffer(afc.read(buffer, Math.abs(rand.nextLong())%(afc.size() - buffer.remaining())), buffer);
				while (nextIndexToHandle <= i && fibb[nextIndexToHandle].future.isDone()) {
					fibb[nextIndexToHandle++].addFaultyOfBufferBytes(totalFacultySum);
				}
			}
			while (nextIndexToHandle < noFaculties) {
				fibb[nextIndexToHandle++].addFaultyOfBufferBytes(totalFacultySum);
			}
			System.out.println("Total faculty sum: " + totalFacultySum.get());
		} finally {
			afc.close();
		}
	}
	
	private static class FutureIntegerByteBuffer {
		
		Future<Integer> future;
		ByteBuffer buffer;
		
		public FutureIntegerByteBuffer(Future<Integer> future, ByteBuffer buffer) {
			super();
			this.future = future;
			this.buffer = buffer;
		}
		
		public void addFaultyOfBufferBytes(AtomicLong totalFacultySum) throws ExecutionException, InterruptedException {
			byte[] bytes = buffer.array();
			int offset = buffer.arrayOffset();
			int noRead = future.get();
			for (int i = 0; i < noRead; i++) {
				faculty((byte)(bytes[offset++]), totalFacultySum);
			}
		}
		
	}
	
	private static void faculty(byte n, AtomicLong addTo) {
		long toAdd = 1;
		for (int i = 1; i <= facultyOf; i++) {
			toAdd *= i;
		}
		addTo.addAndGet(toAdd);
		addTo.addAndGet(n);
	}
	
    private static void createFile(File file, int blockSize, long noBlocks) {
    	try {
            RandomAccessFile f = new RandomAccessFile(file, "rw");
            f.setLength(blockSize * noBlocks);
            f.seek(0);
            byte[] bytes = new byte[blockSize];
            for (long i = 0; i < noBlocks; i++) {
            	rand.nextBytes(bytes);
            	f.write(bytes);
            }
            f.close();
            System.out.println("Created file: " + file + " of size " + file.length());
       } catch (Exception e) {
            e.printStackTrace(System.out);
       }
    }
    
}
