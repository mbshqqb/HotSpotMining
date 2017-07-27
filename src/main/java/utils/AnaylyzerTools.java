package utils;


import org.lionsoul.jcseg.tokenizer.ASegment;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;


import org.lionsoul.jcseg.tokenizer.core.ADictionary;
import org.lionsoul.jcseg.tokenizer.core.DictionaryFactory;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import org.lionsoul.jcseg.tokenizer.core.JcsegException;
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig;
import org.lionsoul.jcseg.tokenizer.core.SegmentFactory;
import spire.math.Interval;

/**
 * Created by Administrator on 2016/4/6.
 */
public class AnaylyzerTools {

    public static String anaylyzerWords(String str){
        JcsegTaskConfig config =new JcsegTaskConfig(AnaylyzerTools.class.getResource("").getPath()+"jcseg.properties");
        ADictionary dic = DictionaryFactory.createDefaultDictionary(config);
        ArrayList<String> list=new ArrayList<String>();
        ASegment seg=null;
        try {
            seg = (ASegment) SegmentFactory.createJcseg(JcsegTaskConfig.COMPLEX_MODE, new Object[]{config, dic});
        } catch (JcsegException e1) {
            e1.printStackTrace();
        }
        try {
            seg.reset(new StringReader(str));
            IWord word = null;
            while ( (word = seg.next()) != null ) {
                list.add(word.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer s = new StringBuffer();
        for(String word:list){
            s.append(word.replace("\u200B"," ").trim()+" ");
        }

        return s.toString();
    }

    public static void main(String[] args) {
        String str="!@##$%$%^&*&*(HBase中通过row和columns确定的为一个存贮单元称为cell。显示每个元素，每个 cell都保存着同一份数据的多个版本。版本通过时间戳来索引。迎泽区是繁华的地方,营业厅营业";
        String strr = str.replace("。","");
        String list=AnaylyzerTools.anaylyzerWords(strr);
        String[] arr = list.split(" ");
        for(String word:arr){
            System.out.println("->"+word+"<-");
        }
        System.out.println("->"+list+"<-");





    }
}