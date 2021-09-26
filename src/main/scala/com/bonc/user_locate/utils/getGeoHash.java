package com.bonc.user_locate.utils;

import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author opslos
 * @date 2021/1/4 11:58:37
 */
public class getGeoHash {


    private static char[] _base32 = {'0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};


    private final static Map<Character, Integer> _decodemap = new HashMap<Character, Integer>();

    static {
        int sz = _base32.length;
        for (int i = 0; i < sz; i++) {
            _decodemap.put(_base32[i], i);
        }
    }


    private static int precision = 12;
    private static int[] bits = {16, 8, 4, 2, 1};


    public static void main(String[] args) throws IOException {
//        getGeoHash ghf = new getGeoHash();
//        String gc1 = ghf.encode(31.277631, 120.53916300000003);
//            String gc2 = ghf.encode(51.4797, -0.0124);
//        BufferedReader bufferedReader = FileUtil.getBufferedReader(new Path("C:\\Users\\liubo\\Desktop\\工作\\贵州\\测试\\经纬度工参\\4g工参.csv"));
        BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Users\\liubo\\Desktop\\工作\\贵州\\测试\\经纬度工参\\4g工参")));
        ArrayList<String> strings = new ArrayList<>();
        String str = "";
        str = br.readLine();
        while (str != null){
//            System.out.println(str);
            String[] split = str.split(",");
            String eci = split[0];
            String lon = split[1];
            String lat = split[2];
            String encode = getGeoHash.encode(Double.parseDouble(lon), Double.parseDouble(lat));
            strings.add(eci+"\t"+encode+"\r");
            str = br.readLine();
        }
        File file =new File("C:\\Users\\liubo\\Desktop\\工作\\贵州\\测试\\经纬度工参\\out");

        if(!file.exists()){
            file.createNewFile();
        }

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));

        for (String string : strings) {

            bufferedWriter.write(string);
        }

        bufferedWriter.close();

//            System.out.println(gc1);
//            System.out.println(gc2);


//            double[] gd1 = ghf.decode(gc1);
//            double[] gd2 = ghf.decode(gc2);
//            System.out.println(gd1[0] + ", " + gd1[1]);
//            System.out.println(gd2[0] + ", " + gd2[1]);
    }


    public static String encode(double latitude, double longitude) {
        double[] lat_interval = {-90.0, 90.0};
        double[] lon_interval = {-180.0, 180.0};


        StringBuilder geohash = new StringBuilder();
        boolean is_even = true;
        int bit = 0, ch = 0;


        while (geohash.length() < precision) {
            double mid = 0.0;
            if (is_even) {
                mid = (lon_interval[0] + lon_interval[1]) / 2;
                if (longitude > mid) {
                    ch |= bits[bit];
                    lon_interval[0] = mid;
                } else {
                    lon_interval[1] = mid;
                }


            } else {
                mid = (lat_interval[0] + lat_interval[1]) / 2;
                if (latitude > mid) {
                    ch |= bits[bit];
                    lat_interval[0] = mid;
                } else {
                    lat_interval[1] = mid;
                }
            }


            is_even = is_even ? false : true;


            if (bit < 4) {
                bit++;
            } else {
                geohash.append(_base32[ch]);
                bit = 0;
                ch = 0;
            }
        }


        return geohash.toString();
    }


    public static double[] decode(String geohash) {
        double[] ge = decode_exactly(geohash);
        double lat, lon, lat_err, lon_err;
        lat = ge[0];
        lon = ge[1];
        lat_err = ge[2];
        lon_err = ge[3];


        double lat_precision = Math.max(1, Math.round(-Math.log10(lat_err))) - 1;
        double lon_precision = Math.max(1, Math.round(-Math.log10(lon_err))) - 1;


        lat = getPrecision(lat, lat_precision);
        lon = getPrecision(lon, lon_precision);


        return new double[]{lat, lon};
    }


    public static double[] decode_exactly(String geohash) {
        double[] lat_interval = {-90.0, 90.0};
        double[] lon_interval = {-180.0, 180.0};


        double lat_err = 90.0;
        double lon_err = 180.0;
        boolean is_even = true;
        int sz = geohash.length();
        int bsz = bits.length;
        double latitude, longitude;
        for (int i = 0; i < sz; i++) {


            int cd = _decodemap.get(geohash.charAt(i));


            for (int z = 0; z < bsz; z++) {
                int mask = bits[z];
                if (is_even) {
                    lon_err /= 2;
                    if ((cd & mask) != 0) {
                        lon_interval[0] = (lon_interval[0] + lon_interval[1]) / 2;
                    } else {
                        lon_interval[1] = (lon_interval[0] + lon_interval[1]) / 2;
                    }


                } else {
                    lat_err /= 2;


                    if ((cd & mask) != 0) {
                        lat_interval[0] = (lat_interval[0] + lat_interval[1]) / 2;
                    } else {
                        lat_interval[1] = (lat_interval[0] + lat_interval[1]) / 2;
                    }
                }
                is_even = is_even ? false : true;
            }


        }
        latitude = (lat_interval[0] + lat_interval[1]) / 2;
        longitude = (lon_interval[0] + lon_interval[1]) / 2;


        return new double[]{latitude, longitude, lat_err, lon_err};
    }


    static double getPrecision(double x, double precision) {
        double base = Math.pow(10, -precision);
        double diff = x % base;
        return x - diff;
    }

}
