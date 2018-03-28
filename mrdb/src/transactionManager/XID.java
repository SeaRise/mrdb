package transactionManager;

public class XID {
	
	final byte active = 0;
	final byte commit = 1;
	final byte aborted = 1<<1;
	
	
}
