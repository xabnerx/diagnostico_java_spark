package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = windowFunctionNationalityOverall(df);
        df = addColumnPotentialVsOverall(df);
        df = windowFunctionPlayeCatNationalityOverall(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                playerCat.column(),
                potentialVsOverall.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> windowFunctionNationalityOverall(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column())
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(20), "B")
                .when(rank.$less(50), "C")
                .otherwise("D");

        df = df.withColumn(playerCat.getName(), rule);

        return df;
    }
    
    private Dataset<Row> addColumnPotentialVsOverall(Dataset<Row> df) {
    		
        return df.withColumn(potentialVsOverall.getName(), 
        		col(potential.getName()).divide(col(overall.getName())));
    }

    
    private Dataset<Row> windowFunctionPlayeCatNationalityOverall(Dataset<Row> df) {
       
    	
    	
    	df = df.filter(col(playerCat.getName()).equalTo("A"));
    	
		/*
		 * WindowSpec w = Window .partitionBy(nationality.column());
		 * 
		 * 
		 * Column rank = rank().over(w);
		 * 
		 * Column rule = when(rank.$less(10), "A") .when(rank.$less(20), "B")
		 * .when(rank.$less(50), "C") .otherwise("D");
		 * 
		 * df = df.withColumn(nationality.getName(), rule);
		 */

        return df;
    }



}
