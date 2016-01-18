import sun.misc.Unsafe;
import java.lang.reflect.*;

interface DataSource
{
    public int available();
    public boolean next(long dst);
    public int nextTile(long dst, int tileSize);
}

class JavaDataSource implements DataSource
{
    int curr;
    int size;
    Unsafe unsafe;

    public int available() {
	return size - curr;
    }
    
    public JavaDataSource(int length) throws Exception {
	curr = 0;
	size = length;
	Constructor cc = Unsafe.class.getDeclaredConstructor();
	cc.setAccessible(true);
	unsafe = (Unsafe)cc.newInstance();	    
    }
    
    public boolean next(long dst)
    {
	if (curr != size) {
	    curr += 1;
	    unsafe.putLong(dst + 0, 0);
	    unsafe.putInt(dst + 8, 1);
	    unsafe.putInt(dst + 12, 2);
	    unsafe.putInt(dst + 16, 3);
	    unsafe.putDouble(dst + 24, 4);
	    unsafe.putDouble(dst + 32, 5);
	    unsafe.putDouble(dst + 40, 6);
	    unsafe.putDouble(dst + 48, 7);
	    unsafe.putByte(dst + 56, (byte)8);
	    unsafe.putByte(dst + 57, (byte)9);
	    unsafe.putInt(dst + 60, 10);
	    unsafe.putInt(dst + 64, 11);
	    unsafe.putInt(dst + 68, 12);
	    return true;
	}
	return false;
    }
    public int nextTile(long dst, int tileSize)
    {
	int n = size - curr < tileSize ? size - curr : tileSize;
	for (int i = 0; i < n; i++) {
	    unsafe.putLong(dst + 0, 0);
	    unsafe.putInt(dst + 8, 1);
	    unsafe.putInt(dst + 12, 2);
	    unsafe.putInt(dst + 16, 3);
	    unsafe.putDouble(dst + 24, 4);
	    unsafe.putDouble(dst + 32, 5);
	    unsafe.putDouble(dst + 40, 6);
	    unsafe.putDouble(dst + 48, 7);
	    unsafe.putByte(dst + 56, (byte)8);
	    unsafe.putByte(dst + 57, (byte)9);
	    unsafe.putInt(dst + 60, 10);
	    unsafe.putInt(dst + 64, 11);
	    unsafe.putInt(dst + 68, 12);
	    dst += 72;
	}
	curr += n;
	return n;	
    }
}

class CppEngine
{
    native double compute(DataSource iterator);
    native double computeTile(DataSource iterator);
}

class ProxyDataSource implements DataSource {
    DataSource in;
    
    ProxyDataSource(DataSource in) {
	this.in = in;
    }

    public int available() {
	return in.available();
    }
    
    public boolean next(long dst)
    {
	return in.next(dst);
    }
    
    public int nextTile(long dst, int tileSize)
    {
	int i;
	for (i = 0; i < tileSize && in.next(dst); i++, dst += 72);
	return i;
    }
}    
    

public class JavaEngine
{
    public static void main(String[] args) throws Exception
    {
	System.loadLibrary("cppengine");
	CppEngine engine  = new CppEngine();
	int n = args.length > 0 ? Integer.parseInt(args[0]) : 100000000;
	long start = System.currentTimeMillis();
	double sum = engine.compute(new JavaDataSource(n));
	System.out.println("Elapsed time: " + (System.currentTimeMillis() - start) + ", sum=" + sum);
	start = System.currentTimeMillis();
	sum = engine.computeTile(new JavaDataSource(n));
	System.out.println("Elapsed time: " + (System.currentTimeMillis() - start) + ", sum=" + sum);
	start = System.currentTimeMillis();
	sum = engine.computeTile(new ProxyDataSource(new JavaDataSource(n)));
	System.out.println("Elapsed time: " + (System.currentTimeMillis() - start) + ", sum=" + sum);
    }
}
