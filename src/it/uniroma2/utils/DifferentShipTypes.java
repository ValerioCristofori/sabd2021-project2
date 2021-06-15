package it.uniroma2.utils;

public class DifferentShipTypes {
    private static final String MILITARE = "Militare";
    private static final String TRASPORTO_PASSEGGERI = "Trasporto_Passeggeri";
    private static final String CARGO = "Cargo";
    private static final String OTHERS = "Altro";



    public static String getTypeShip( Integer value ){
        if(value == 35)
                return MILITARE;
        else if( value <= 69 && value >= 60 )
                return TRASPORTO_PASSEGGERI;
        else if ( value <= 79 && value >= 70 )
                return CARGO;
        else
                return OTHERS;
    }

}
