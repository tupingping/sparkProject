package cn.tpp.sparkProject.test;

public class Sigleton {
	private static Sigleton instance = null;	
	private Sigleton(){}
	public Sigleton getInstance(){
		if(instance == null){
			synchronized (Sigleton.class) {
				if(instance == null){
					instance = new Sigleton();
				}
			}
		}
		return instance;
	}
}
