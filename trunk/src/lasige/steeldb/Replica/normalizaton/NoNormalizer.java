package lasige.steeldb.Replica.normalizaton;

public class NoNormalizer implements Normalizer {

	@Override
	public String normalize(String sql) {
		return sql;
	}

}
