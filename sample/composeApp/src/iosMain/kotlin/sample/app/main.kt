import androidx.compose.ui.window.ComposeUIViewController
import com.darkrockstudios.libs.meshcore.ble.BlueFalconBleAdapter
import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.engine.ios.IosEngine
import platform.UIKit.UIViewController
import sample.app.App

fun MainViewController(): UIViewController = ComposeUIViewController {
	val blueFalcon = BlueFalcon(IosEngine())
	App(BlueFalconBleAdapter(blueFalcon))
}
