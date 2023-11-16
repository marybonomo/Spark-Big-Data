package project_spark;

public class JavaPerson {
    private static String nome;
    private static String cognome;
    private static int giorno;
    private static int mese;
    private static int anno;

    public JavaPerson(String nome, String cognome, int giorno, int mese, int anno) {
        this.nome = nome;
        this.cognome = cognome;
        this.giorno = giorno;
        this.mese = mese;
        this.anno = anno;

    }

    public int getAnno() {
        return anno;
    }

    public String getCognome() {
        return cognome;
    }

    public int getGiorno() {

        return giorno;
    }

    public int getMese() {
        
        return mese;
    }

    public String getNome() {
        return nome;
    }



    @Override
public boolean equals(Object persona)
    {
    	JavaPerson dacontrollare=(JavaPerson)persona;

if(dacontrollare.getNome().equals(nome)&& dacontrollare.getCognome().equals(cognome)&& dacontrollare.getGiorno()==giorno && dacontrollare.getMese()==mese  && dacontrollare.getAnno()==anno)
    return true;

return false;
}

    @Override
    public String toString() {
        return "Persona{" + "nome=" + nome + "cognome=" + cognome + "giorno=" + giorno + "mese=" + mese + "anno=" + anno + '}';
    }

	
/*
 public static void main(String[] args) {
        // TODO code application logic here

 //Persona nuova=new Persona("Pippo", "Baudo", 27, 12, 1982);
	 JavaPerson nuova=new JavaPerson("Mario", "Rossi", 27, 12,1990);

 System.out.println(nuova.toString());

 JavaPerson dacontrollare=new JavaPerson("Mario", "Rossi", 27, 12,1990);
 System.out.println(dacontrollare.toString());


 if(nuova.equals(dacontrollare))
     System.out.println("Le due persone sono uguali");
 else
     System.out.println("Le due persone sono diverse");


 }*/

}
