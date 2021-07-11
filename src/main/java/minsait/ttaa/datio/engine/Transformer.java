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
	
	public Transformer(@NotNull SparkSession spark,String input,String output) {
		this.spark = spark;
		
		Dataset<Row> df = readInput(input);

		df.printSchema();

		df = cleanData(df);
		df = windowFunctionNationalityOverall(df);
		df = addColumnPotentialVsOverall(df);
		df = windowFunctionPlayeCatNationalityOverall(df);
		df = columnSelection(df);

		df.show(100, false);
		// for show 100 records after your transformations and show the Dataset schema
		df.printSchema();
		write(df,output);
		
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
	private Dataset<Row> readInput(String input) {
		Dataset<Row> df = spark.read()
				.option(HEADER, true)
				.option(INFER_SCHEMA, true)
				.csv(input);
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
	 * @param df es un conjunto de datos con la información de jugadores
	 * @return agrega al conjunto de datos la columna "player_cat"
	 * se realiza por la siguiente regla
	 * caracter A si el jugador es de los mejores 10 jugadores de su país
	 * caracter B si el jugador es de los mejores 20 jugadores de su país
	 * caracter C si el jugador es de los mejores 50 jugadores de su país
	 * caracter D para el resto de jugadores
	 */
	private Dataset<Row> windowFunctionNationalityOverall(Dataset<Row> df) {
		WindowSpec w = Window
				.partitionBy(nationality.column())
				.orderBy(overall.column().desc());

		Column rank = rank().over(w);

		Column rule = when(rank.$less(10), A)
				.when(rank.$less(20), B)
				.when(rank.$less(50), C)
				.otherwise(D);

		df = df.withColumn(playerCat.getName(), rule);

		return df;
	}

	
	/**
	 * @param df es un conjunto de datos con la información de jugadores
	 * @return agregar al conjunto de datos la columna "potential_vs_overall"
	 * se realiza por la siguiente regla
	 * la columna `potential` dividida por la columna `overall` 
	 */
	private Dataset<Row> addColumnPotentialVsOverall(Dataset<Row> df) {

		return df.withColumn(potentialVsOverall.getName(), 
				col(potential.getName()).divide(col(overall.getName())));
	}


	/**
	 * @param df es un conjunto de datos con la información de jugadores
	 * @return Se realiza un filtro mediante dos coulmnas: la columna "player_cat" y la columna "potential_vs_overall"
	 * se realiza por la siguiente regla
	 * Si "player_cat" esta en los siguientes valores: **A**, **B**
     * Si "player_cat" es **C** y "potential_vs_overall" es superior a **1.15**
     * Si "player_cat" es **D** y "potential_vs_overall" es superior a **1.25**
	 */
	private Dataset<Row> windowFunctionPlayeCatNationalityOverall(Dataset<Row> df) {
		df = df.filter(col(playerCat.getName()).equalTo(A)
				.or(col(playerCat.getName()).equalTo(B))
				.or(col(playerCat.getName()).equalTo(C)
						.and(col(potentialVsOverall.getName()).$greater(1.15)))
				.or(col(playerCat.getName()).equalTo(D)
						.and(col(potentialVsOverall.getName()).$greater(1.25)))
				);

		return df;
	}


}
