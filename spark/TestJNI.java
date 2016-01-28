class NativeClass
{
    native int nativeMethod(JavaClass jc);
}

class JavaClass
{
    int i;
    public int javaMethod() {
        return ++i;
    }
}

public class TestJNI
{
    public static void main(String[] args) {
        System.loadLibrary("native");
        NativeClass nc = new NativeClass();
        System.out.println("Result" + nc.nativeMethod(new JavaClass()));
    }
}
