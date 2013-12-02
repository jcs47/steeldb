package lasige.steeldb.Replica.normalizaton;

/**
 * This interfaces defines the structure of a normalizer class.
 * The idea of a normalizer is to define a class that will convert
 * the commands from the default sql being used to that database
 * specific commands.
 * @author paulo
 *
 */
public interface Normalizer {

	/**
	 * Normalizes an sql command to a certain database syntax
	 * @param sql the command to be normalized.
	 * @return the normalized command.
	 */
	public String normalize(String sql);
}
