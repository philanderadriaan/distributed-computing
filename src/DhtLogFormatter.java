package tcss558.team05;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Locale;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * This class defines how log messages should look like.
 */
class DhtLogFormatter extends Formatter {
    /**
     * date format in a log message
     */
    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS", Locale.US);

    /**
     * @inheritDoc
     */
    @Override
    public String format(final LogRecord r) {
        return formatter.format(new Date())+": "+r.getMessage()+"\r\n";
    }
}