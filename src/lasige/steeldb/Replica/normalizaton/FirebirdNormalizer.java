package lasige.steeldb.Replica.normalizaton;

public class FirebirdNormalizer implements Normalizer {

	@Override
	public String normalize(String sql) {
		String normalized;
		if(sql.startsWith("TRUNCATE TABLE"))
			normalized = sql.replace("TRUNCATE TABLE", "DELETE FROM");
		else
			normalized = sql;
		return normalized;
	}

}
