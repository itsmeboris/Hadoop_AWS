package Ex2.utils;

import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;


public class awsVars {
    public static final String EX2_JAR = "Ex2.jar";
    public static final String INPUT_LOCAL_LOCATION = "E:\\Download\\Compressed\\";
    public static final String INPUT_BUCKET_NAME = "/user/input/";
    public static final String OUTPUT_BUCKET_NAME = "/user/output/";
    public static final String PATH = "googlebooks-eng-all-2gram-20120701-a_";
    public static final String APPLICATION_CODE_BUCKET_NAME = "apllication-code-bucket-dsp202";
    public static final String IAM_PROFILE_NAME = "Application";
    public static final String KEY_PAIR_NAME = "Boris";
    public static final String SECURITY_GROUP = "Boris";
    public static final Region REGION = Region.US_EAST_1;
    public static final String MAIN_CLASS = "Ex2.bigrams.main";
    public static final String JOB_NAME = "Pairs";
    public static final int NUMBER_REDUCERS = 1;
    public static final String ENG = "eng";
    public static final String HEB = "heb";

    public static String[] filterStopWords(String text, String language) {
        text = " " + text + " ";
        text = text.toLowerCase();
        text = text.replaceAll("[^a-z]+", " ");
        if(language == ENG) {
            for (String word : ENGLISH_STOP_WORDS) {
                text = text.replaceAll(" " + word + " ", " ");
            }
        } else {
            // TODO - need to implement for hebrew
            for (String word : ENGLISH_STOP_WORDS) {
                text = text.replaceAll(" " + word + " ", " ");
            }
        }
        text = text.replaceAll("^\\s+", "");
        StringTokenizer st = new StringTokenizer(text);
        ArrayList<String> result = new ArrayList<>();
        while (st.hasMoreTokens())
            result.add(st.nextToken());
        return Arrays.copyOf(result.toArray(),result.size(),String[].class);
    }

    public static boolean checkStopWords(String[] texts){
        for(String text: texts){
            if(Arrays.asList(ENGLISH_STOP_WORDS).contains(text)){
                return false;
            }
        }
        return true;
    }

    public static String getS3Address(String input){
        StringBuilder output = new StringBuilder("s3n://" + APPLICATION_CODE_BUCKET_NAME);
        output.append("/").append(input);
        return output.toString();
    }

    public static final String[] ENGLISH_STOP_WORDS = {"a", "about", "above", "across", "after",
            "afterwards", "again", "against", "all", "almost", "alone", "along", "already",
            "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an",
            "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
            "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been",
            "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both",
            "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe",
            "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc",
            "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for",
            "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her",
            "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed",
            "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill",
            "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none",
            "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves",
            "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should",
            "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system",
            "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon",
            "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top",
            "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what",
            "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether",
            "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with",
            "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"};

    public static HadoopJarStepConfig HadoopJarStepsConfig(){
        return new HadoopJarStepConfig()
                .withJar(getS3Address(EX2_JAR))
                .withMainClass(MAIN_CLASS)
                .withArgs(getS3Address(INPUT_BUCKET_NAME), getS3Address(OUTPUT_BUCKET_NAME));
    }
}