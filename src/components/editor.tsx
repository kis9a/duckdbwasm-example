import { useEffect, useRef } from "preact/hooks";
import { EditorView, basicSetup } from "codemirror";
import { EditorState } from "@codemirror/state";
import { sql } from "@codemirror/lang-sql";
import { vim } from "@replit/codemirror-vim";

interface EditorProps {
  initialValue: string;
  isVimMode: boolean;
  onChange?: (value: string) => void;
}

export default function Editor({
  initialValue,
  isVimMode,
  onChange,
}: EditorProps) {
  const editorContainerRef = useRef<HTMLDivElement>(null);
  const editorViewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!editorContainerRef.current) return;

    editorViewRef.current?.destroy();

    const newEditorView = new EditorView({
      state: EditorState.create({
        doc: initialValue,
        extensions: [
          basicSetup,
          sql(),
          isVimMode ? vim({ status: true }) : [],
          EditorState.readOnly.of(false),
          EditorView.lineWrapping,
          EditorView.updateListener.of((update) => {
            if (update.docChanged) {
              const doc = update.state.doc.toString();
              onChange?.(doc);
            }
          }),
        ],
      }),
      parent: editorContainerRef.current,
    });

    editorViewRef.current = newEditorView;

    return () => {
      newEditorView.destroy();
    };
  }, [initialValue, isVimMode, onChange]);

  return <div ref={editorContainerRef} class="editor-container"></div>;
}
